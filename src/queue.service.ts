import {
  Injectable,
  OnModuleInit,
  OnModuleDestroy,
  Logger,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import * as PgBoss from 'pg-boss';
import type { WorkHandler } from 'pg-boss';

export interface ExtendedWorkOptions {
  newJobCheckInterval?: number;
  singletonKey?: string;
  retryLimit?: number;
  retryDelay?: number;
  interval?: number;
}

@Injectable()
export class QueueService implements OnModuleInit, OnModuleDestroy {
  private boss: PgBoss;
  private readonly logger = new Logger(QueueService.name);
  private isInitialized = false;
  private initPromise: Promise<void>;

  constructor(private configService: ConfigService) {
    const dbConfig = {
      host: this.configService.get('DB_HOST'),
      port: parseInt(this.configService.get('DB_PORT')),
      database: this.configService.get('DB_DATABASE'),
      user: this.configService.get('DB_USERNAME'),
      password: this.configService.get('DB_PASSWORD'),
    };

    this.logger.log('Initializing PgBoss with config:', {
      ...dbConfig,
      password: '***',
    });

    this.boss = new PgBoss({
      host: dbConfig.host,
      port: dbConfig.port,
      database: dbConfig.database,
      user: dbConfig.user,
      password: dbConfig.password,
      schema: 'pgboss',
      application_name: 'deepflow-queue',
    });

    this.boss.on('error', (error) => {
      this.logger.error('PgBoss error:', error);
    });

    this.initPromise = this.initialize();
  }

  private async initialize() {
    try {
      this.logger.log('Starting PgBoss initialization...');

      // 检查数据库配置
      const dbConfig = {
        host: this.configService.get('DB_HOST'),
        port: parseInt(this.configService.get('DB_PORT')),
        database: this.configService.get('DB_DATABASE'),
        user: this.configService.get('DB_USERNAME'),
      };
      this.logger.log('Database configuration:', { ...dbConfig });
      // 尝试启动 pg-boss
      try {
        await this.boss.start();
      } catch (startError) {
        this.logger.error('Failed to start PgBoss:', {
          error: startError.message,
          stack: startError.stack,
        });
        throw startError;
      }
      // 等待表创建完成
      await new Promise((resolve) => setTimeout(resolve, 10000));

      this.isInitialized = true;
      this.logger.log('PgBoss started and verified successfully');
    } catch (error) {
      this.logger.error('Failed to initialize PgBoss:', {
        error: error.message,
        stack: error.stack,
        dbConfig: {
          host: this.configService.get('DB_HOST'),
          port: this.configService.get('DB_PORT'),
          database: this.configService.get('DB_DATABASE'),
          user: this.configService.get('DB_USERNAME'),
        },
      });
      throw error;
    }
  }

  async onModuleInit() {
    await this.initPromise;
  }

  async onModuleDestroy() {
    if (this.isInitialized) {
      await this.boss.stop();
      this.logger.log('PgBoss stopped');
    }
  }

  private async ensureInitialized() {
    if (!this.isInitialized) {
      await this.initPromise;
    }
  }

  async addJob<T extends object>(
    queue: string,
    data: T,
    options: PgBoss.SendOptions = {},
  ): Promise<string> {
    try {
      await this.ensureInitialized();

      // 检查队列状态
      const queues = await this.boss.getQueues();
      this.logger.debug(`Current queues:`, queues);

      this.logger.debug(`Adding job to queue ${queue}:`, {
        data,
        options,
        isInitialized: this.isInitialized,
      });
      // 初始化queue
      await this.boss.createQueue(queue);

      // 尝试发送任务
      const jobId = await this.boss.send(queue, data, {
        retryLimit: 3,
        retryDelay: 5000,
        ...options,
      });
      this.logger.debug(`Job added successfully with ID: ${jobId}`);
      if (!jobId) {
        this.logger.error('Failed to add job: received null jobId', {
          queue,
          data,
          options,
          queues,
        });
        throw new Error('Failed to add job to queue: received null jobId');
      }

      this.logger.debug(`Job added successfully with ID: ${jobId}`);
      return jobId;
    } catch (error) {
      this.logger.error(`Failed to add job to queue ${queue}:`, {
        error: error.message,
        stack: error.stack,
        data,
        options,
      });
      throw error;
    }
  }

  // 处理队列中的任务
  async processJob<T>(
    queue: string,
    handler: (data: T) => Promise<void>,
    options: ExtendedWorkOptions = {},
  ): Promise<string> {
    await this.ensureInitialized();

    try {
      const { newJobCheckInterval, ...bossOptions } = options;
      const wrappedHandler: WorkHandler<T> = async (jobs) => {
        const job = jobs[0]; // pg-boss 总是发送一个 job 数组，我们处理第一个
        if (!job?.data) {
          this.logger.warn(`Invalid job received from queue ${queue}`);
          return;
        }
        try {
          await handler(job.data);
        } catch (error) {
          this.logger.error(`Error processing job in queue ${queue}:`, {
            error: error.message,
            stack: error.stack,
          });
          throw error;
        }
      };

      const workOptions = {
        ...bossOptions,
        interval: newJobCheckInterval || 5000,
        includeMetadata: false,
        teamSize: 1,
        teamConcurrency: 1,
      };

      const workerId = await this.boss.work(
        queue,
        workOptions as PgBoss.WorkOptions,
        wrappedHandler,
      );

      this.logger.debug(
        `Worker registered for queue ${queue} with ID: ${workerId}`,
      );
      return workerId;
    } catch (error) {
      this.logger.error(`Failed to register worker for queue ${queue}:`, {
        error: error.message,
        stack: error.stack,
      });
      throw error;
    }
  }

  // 调度定时任务
  async schedule<T extends object>(
    queue: string,
    data: T,
    cron: string,
    options: PgBoss.ScheduleOptions = {},
  ): Promise<string> {
    const jobId = await this.boss.schedule(queue, cron, data, options);
    if (typeof jobId !== 'string') {
      throw new Error(`Failed to schedule job for queue ${queue}`);
    }
    return jobId;
  }
}
