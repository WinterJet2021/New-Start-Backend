# NewStart Core Backend

The **NewStart Core Backend** is the central engine of the NewStart Scheduling System.  
It manages schedule generation, model building, NLU processing, solver execution, KPI evaluation, and data persistence.  
The Core Backend exposes the **official API contract** for the system and is deployed independently from the BFF (Backend For Frontend).

---

## 1. System Responsibilities

The Core Backend powers all core scheduling intelligence:

### HTTP API (NestJS)
- Provides REST endpoints for:
  - Schedules
  - Solver runs
  - Staff management
  - KPIs
  - Internal admin/ops actions

### Chatbot NLU (Rasa)
- Receives webhook messages from the LINE OA chatbot (via reverse proxy).
- Classifies user intents and entities.
- Delegates actions to:
  - Manager workers (normalization / modelling / evaluation)
  - API endpoints in the NestJS service

### Manager Workers (Python)
- Data normalization
- Model construction (building solver input models)
- Evaluation & KPI generation

### Solver Engine (Python)
- **CP-SAT solver (OR-Tools)** – Plan A
- **Gurobi solver** – Plan B / alternative engine

### Database Layer (PostgreSQL)
- Central storage for:
  - Organizations, units, staff
  - Schedules and assignments
  - Solver runs and metadata
  - KPIs and audit logs

All of the above are orchestrated using **Docker Compose** in development and production.

---

## 2. Architecture Overview

High-level flow for the Core Backend:

- BFF / Web Application → calls the **NestJS API** for all core backend features.
- LINE OA chatbot → sends webhook events through a **reverse proxy** → **Rasa NLU** → calls back into Manager or NestJS.
- Manager workers (Python) → handle data normalization, model building, and KPI calculation.
- Solver Engine (Python) → runs:
  - OR-Tools CP-SAT solver
  - Gurobi solver
- PostgreSQL (AWS RDS in production, Docker in dev) → stores all persistent data.
- Reverse proxy (NGINX / Traefik) → routes:
  - `/api/*` → NestJS API
  - `/line-webhook` → Rasa server
  - Keeps internal workers and database private.

All external traffic enters through the reverse proxy; internal services are not directly exposed.

---

## 3. Technology Stack

### API Layer
- **NestJS** (Node.js + TypeScript)
- REST-style endpoints
- DTOs, guards, interceptors, and filters for validation and security

### NLU Layer
- **Rasa** (Python-based NLU)
- Intent classification and entity extraction
- LINE OA integration via webhook and actions

### Worker Layer
- **Python 3**
- Manager service for:
  - Normalization
  - Model building
  - KPI evaluation

### Solver Layer
- **OR-Tools CP-SAT** (Google OR-Tools)
- **Gurobi Optimizer**

### Database
- **PostgreSQL**
  - Production: AWS RDS
  - Development: Local Docker container

### Infrastructure
- **Docker** and **Docker Compose**
- **Reverse proxy** (NGINX/Traefik) for routing and TLS termination

---

## 4. Repository Structure

The Core Backend uses a conventional multi-service backend layout with clear boundaries:

    newstart-core-backend/
    ├── api/                           # NestJS HTTP API service
    │   ├── src/
    │   │   ├── modules/               # Feature modules (schedules, staff, solver-runs, etc.)
    │   │   ├── common/                # Interceptors, guards, filters, pipes, DTOs
    │   │   ├── config/                # ConfigService + environment loaders
    │   │   ├── main.ts                # NestJS bootstrap
    │   │   └── app.module.ts
    │   ├── test/                      # API unit & integration tests
    │   ├── package.json
    │   └── tsconfig.json
    │
    ├── rasa/                          # Rasa NLU project
    │   ├── data/                      # Training data (intents, stories)
    │   ├── domain.yml                 # Intent/entity schema
    │   ├── config.yml                 # NLU pipeline configuration
    │   ├── actions/                   # Custom Python actions (optional)
    │   └── models/                    # Exported trained models
    │
    ├── manager/                       # Manager service (Python)
    │   ├── src/
    │   │   ├── normalization/         # Input cleaning and normalization
    │   │   ├── model_builder/         # Build solver-ready models
    │   │   ├── evaluation/            # KPI & quality metrics
    │   │   ├── utils/                 # Shared utilities
    │   │   └── app.py                 # Worker entrypoint
    │   ├── requirements.txt
    │   └── Dockerfile
    │
    ├── solver/                        # Solver Engine (Python)
    │   ├── cpsat_worker/
    │   │   ├── src/
    │   │   │   ├── model/             # CP-SAT model definitions
    │   │   │   ├── solver/            # OR-Tools solving logic
    │   │   │   └── utils/             # Solver utilities
    │   │   ├── requirements.txt
    │   │   └── Dockerfile
    │   └── gurobi_worker/
    │       ├── src/
    │       │   ├── model/             # Gurobi model definitions
    │       │   ├── solver/            # Gurobi optimization logic
    │       │   └── utils/
    │       ├── requirements.txt
    │       └── Dockerfile
    │
    ├── db/
    │   ├── migrations/                # SQL or migration-tool scripts
    │   ├── seeds/                     # Dev seed data
    │   └── schema/                    # Initial schema definitions
    │
    ├── openapi/                       # Source-of-truth API contract
    │   ├── newstart-core.yaml
    │   └── README.md
    │
    ├── docker/                        # Infra configs (optional)
    │   ├── nginx/                     # Reverse proxy configs
    │   ├── scripts/                   # Helper scripts
    │   └── monitoring/                # Monitoring / logging configs
    │
    ├── docker-compose.yml             # Full backend stack definition
    ├── .env.example                   # Example environment configuration
    ├── Makefile                       # Optional local automation commands
    └── README.md

---

## 5. Environment Configuration

1. Copy the example environment file:

       cp .env.example .env

2. Configure values inside `.env`, such as:
   - PostgreSQL connection string / credentials
   - Rasa server URL
   - Solver configuration (e.g., which engine to use by default)
   - Internal service URLs (manager, solver workers)
   - Ports for API, workers, and NLU
   - Any development-only secrets or tokens

Do **not** commit real secrets or production credentials to the repository.

---

## 6. Running the Stack (Local Development)

1. Build and start all services:

       docker compose up --build

2. Expected running services:
   - NestJS API → `http://localhost:<API_PORT>`
   - Rasa server → `http://localhost:<RASA_PORT>`
   - Manager workers (Python)
   - CP-SAT worker
   - Gurobi worker
   - Local PostgreSQL

3. To stop the stack:

       docker compose down

You can then use tools like Postman, Bruno, or curl to hit the local API and verify integration.

---

## 7. Deployment Model (Production)

The Core Backend is designed to run primarily on AWS:

### EC2 Instance
- Hosts the Docker Compose stack:
  - NestJS API
  - Rasa NLU
  - Manager workers
  - Solver workers (CP-SAT and Gurobi)
- Logs and metrics can be shipped to CloudWatch or other monitoring tools.

### AWS RDS (PostgreSQL)
- Serves as the main datastore.
- Only accessible from the EC2 instance (or a private network).
- Protected by security groups and network policies.

### Reverse Proxy (NGINX / AWS ALB)
- Routes external traffic:
  - `/api/*` → NestJS API
  - `/line-webhook` → Rasa endpoint
- Terminates HTTPS and manages certificates.
- Ensures that:
  - Manager workers
  - Solver workers
  - Database
  remain internal and are not directly exposed.

---

## 8. API Contract (OpenAPI)

The Core Backend is the **source of truth** for the API used by the BFF and other clients.

- OpenAPI specification file:

      openapi/newstart-core.yaml

- Update this file whenever:
  - A new endpoint is added.
  - Request or response models change.
  - Existing routes are deprecated or significantly modified.

The BFF consumes this contract to generate types and client bindings, keeping frontend and backend aligned.

---

## 9. Testing

### NestJS API Tests

Run unit and integration tests for the API:

    cd api
    npm run test

### Python Worker Tests

Run tests for the Manager and Solver workers:

    cd manager
    pytest

    cd ../solver/cpsat_worker
    pytest

    cd ../gurobi_worker
    pytest

### End-to-End (E2E) Testing

With the full stack running via `docker compose`, use tools such as:

- Postman / Bruno / Hoppscotch
- curl or HTTPie

to exercise flows like:

- Creating or updating schedules
- Triggering solver runs
- Checking solver results and KPIs
- Validating Rasa → Manager → API workflows

---

## 10. Contributing Guidelines

- Use feature branches and Pull Requests for all changes.
- Keep commit messages clear and descriptive.
- For any API-related change:
  - Update `openapi/newstart-core.yaml`.
  - Ensure DTOs, validators, and docs match.

### Code Style

- **NestJS / TypeScript:**
  - Use ESLint and Prettier.
- **Python (Manager / Solver):**
  - Use Black and flake8.

### Documentation

- Keep this `README.md` up to date as the architecture and workflows evolve.
- When adding a new service or major feature:
  - Document its purpose.
  - Document any new environment variables.
  - Update diagrams and the OpenAPI spec if relevant.

---

## 11. License

Add your chosen license here, for example:

- MIT
- Apache-2.0
- GPLv3
- Internal / proprietary license

---

This README documents the NewStart Core Backend system, including its responsibilities, services, repository structure, configuration, deployment model, and contribution guidelines.