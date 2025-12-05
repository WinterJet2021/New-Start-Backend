import { Test, TestingModule } from '@nestjs/testing';
import { SolverEngineService } from './solver-engine.service';

describe('SolverEngineService', () => {
  let service: SolverEngineService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [SolverEngineService],
    }).compile();

    service = module.get<SolverEngineService>(SolverEngineService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
