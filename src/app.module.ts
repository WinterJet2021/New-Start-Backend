import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { NluModule } from './nlu/nlu.module';
import { ManagerModule } from './manager/manager.module';
import { SolverEngineModule } from './solver-engine/solver-engine.module';

@Module({
  imports: [NluModule, ManagerModule, SolverEngineModule],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
