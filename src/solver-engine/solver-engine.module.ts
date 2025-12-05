import { Module } from '@nestjs/common';
import { SolverEngineController } from './solver-engine.controller';
import { SolverEngineService } from './solver-engine.service';

@Module({
  controllers: [SolverEngineController],
  providers: [SolverEngineService]
})
export class SolverEngineModule {}
