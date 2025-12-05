import { Test, TestingModule } from '@nestjs/testing';
import { SolverEngineController } from './solver-engine.controller';

describe('SolverEngineController', () => {
  let controller: SolverEngineController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [SolverEngineController],
    }).compile();

    controller = module.get<SolverEngineController>(SolverEngineController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});
