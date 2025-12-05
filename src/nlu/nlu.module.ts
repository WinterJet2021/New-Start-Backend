import { Module } from '@nestjs/common';
import { NluController } from './nlu.controller';
import { NluService } from './nlu.service';

@Module({
  controllers: [NluController],
  providers: [NluService]
})
export class NluModule {}
