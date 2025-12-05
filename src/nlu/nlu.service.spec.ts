import { Test, TestingModule } from '@nestjs/testing';
import { NluService } from './nlu.service';

describe('NluService', () => {
  let service: NluService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [NluService],
    }).compile();

    service = module.get<NluService>(NluService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
