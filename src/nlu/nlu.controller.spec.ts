import { Test, TestingModule } from '@nestjs/testing';
import { NluController } from './nlu.controller';

describe('NluController', () => {
  let controller: NluController;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      controllers: [NluController],
    }).compile();

    controller = module.get<NluController>(NluController);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });
});
