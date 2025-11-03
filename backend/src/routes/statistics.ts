import { Router } from 'express';
import { MongoDBConnector } from '../services/mongodb';
import { SQLiteManager } from '../services/sqlite';
import { GatewayMonitor } from '../services/gateway';
import { EncoderLookupService } from '../services/encoder-lookup';
import { DiscordWebhookService } from '../services/discord-webhook';
import { logger } from '../utils/logger';
import { ApiResponse, DailyStatistics, PerformanceAnalytics } from '../types/index';
import { config } from '../config/index';

const router = Router();

// Services will be initialized lazily to avoid startup issues
let mongodb: MongoDBConnector;
let sqliteManager: SQLiteManager;
let gatewayMonitor: GatewayMonitor | null = null;
let encoderLookup: EncoderLookupService;
let discordWebhook: DiscordWebhookService;

const getServices = () => {
  if (!mongodb) {
    mongodb = MongoDBConnector.getInstance();
    sqliteManager = SQLiteManager.getInstance();
    encoderLookup = new EncoderLookupService(mongodb, sqliteManager);
    discordWebhook = new DiscordWebhookService();
  }
  return { mongodb, sqliteManager, encoderLookup, discordWebhook };
};

const getGatewayMonitor = () => {
  if (!gatewayMonitor) {
    gatewayMonitor = new GatewayMonitor();
  }
  return gatewayMonitor;
};

/**
 * GET /api/statistics/daily
 * Get daily encoding statistics
 */
router.get('/daily', async (req, res) => {
  try {
    const { mongodb } = getServices();
    const days = Math.min(parseInt(req.query.days as string) || 30, 365);
    
    const mongoStats = await mongodb.getDailyStatistics(days);
    
    // Transform MongoDB aggregation results to our interface
    const dailyStats: DailyStatistics[] = mongoStats.map((stat: any) => {
      // Process by_encoder array to create object
      const byEncoder: Record<string, number> = {};
      stat.by_encoder.forEach((item: any) => {
        if (item.encoder_id) {
          byEncoder[item.encoder_id] = item.count;
        }
      });

      // Process by_quality array to create object
      const byQuality: Record<string, number> = {};
      stat.by_quality.forEach((item: any) => {
        if (item.quality) {
          byQuality[item.quality] = item.count;
        }
      });

      return {
        date: stat._id,
        videos_encoded: stat.videos_encoded,
        by_encoder: byEncoder,
        by_quality: byQuality,
        average_encoding_time: Math.round(stat.average_encoding_time || 0),
        success_rate: 1.0, // TODO: Calculate from completed vs failed
        total_encoding_time: Math.round(stat.total_encoding_time || 0)
      };
    });

    const response: ApiResponse<DailyStatistics[]> = {
      success: true,
      data: dailyStats
    };

    res.json(response);
  } catch (error) {
    logger.error('Error fetching daily statistics', error);
    const response: ApiResponse = {
      success: false,
      error: 'Failed to fetch daily statistics'
    };
    res.status(500).json(response);
  }
});

/**
 * GET /api/statistics/encoders
 * Get per-encoder statistics
 */
router.get('/encoders', async (req, res) => {
  try {
    const { mongodb, sqliteManager, encoderLookup } = getServices();
    const days = Math.min(parseInt(req.query.days as string) || 7, 365);
    const encoderId = req.query.encoder_id as string;
    
    const mongoPerformance = await mongodb.getEncoderPerformance(encoderId);
    const sqliteStats = await sqliteManager.getEncoderStats(encoderId, days);

    // Get encoder DID keys for lookup
    const encoderDids = mongoPerformance.map((perf: any) => perf._id);
    const encoderInfoMap = await encoderLookup.getMultipleEncoderInfos(encoderDids);

    // Combine MongoDB and SQLite data with encoder names
    const encoderStats = mongoPerformance.map((perf: any) => {
      const sqliteData = sqliteStats.find(s => s.encoder_id === perf._id);
      const encoderInfo = encoderInfoMap.get(perf._id);
      
      return {
        encoder_id: perf._id,
        encoder_name: encoderInfo?.nodeName,
        jobs_completed: perf.jobs_completed,
        total_encoding_time: Math.round(perf.total_encoding_time || 0),
        average_encoding_time: Math.round(perf.average_encoding_time || 0),
        success_rate: Math.round((perf.success_rate || 0) * 100) / 100,
        daily_stats: sqliteData || null
      };
    });

    const response: ApiResponse = {
      success: true,
      data: encoderStats
    };

    res.json(response);
  } catch (error) {
    logger.error('Error fetching encoder statistics', error);
    const response: ApiResponse = {
      success: false,
      error: 'Failed to fetch encoder statistics'
    };
    res.status(500).json(response);
  }
});

/**
 * GET /api/statistics/performance
 * Get overall performance analytics
 */
router.get('/performance', async (req, res) => {
  try {
    const { mongodb } = getServices();
    const days = Math.min(parseInt(req.query.days as string) || 7, 365);
    
    // Get encoder performance data
    const encoderPerformance = await mongodb.getEncoderPerformance();
    
    // Calculate encoder efficiency (jobs per hour)
    const encoderEfficiency: Record<string, number> = {};
    encoderPerformance.forEach((perf: any) => {
      const hoursInPeriod = days * 24;
      encoderEfficiency[perf._id] = Math.round((perf.jobs_completed / hoursInPeriod) * 100) / 100;
    });

    // TODO: Get hardware vs software encoding ratios from actual job data
    const hardwareUtilization = {
      hardware_encoding: 75, // Placeholder percentage
      software_encoding: 25  // Placeholder percentage
    };

    // TODO: Calculate queue performance metrics
    const queuePerformance = {
      average_wait_time: 300, // Placeholder: 5 minutes in seconds
      current_queue_depth: 0, // Would get from gateway
      peak_queue_depth: 10   // Placeholder
    };

    const performanceAnalytics: PerformanceAnalytics = {
      encoder_efficiency: encoderEfficiency,
      hardware_utilization: hardwareUtilization,
      queue_performance: queuePerformance
    };

    const response: ApiResponse<PerformanceAnalytics> = {
      success: true,
      data: performanceAnalytics
    };

    res.json(response);
  } catch (error) {
    logger.error('Error fetching performance analytics', error);
    const response: ApiResponse = {
      success: false,
      error: 'Failed to fetch performance analytics'
    };
    res.status(500).json(response);
  }
});

/**
 * GET /api/statistics/summary
 * Get overall system summary statistics
 */
router.get('/summary', async (req, res) => {
  try {
    const { mongodb, sqliteManager } = getServices();
    const [dailyStats, encoderPerf] = await Promise.all([
      mongodb.getDailyStatistics(1), // Today's stats
      mongodb.getEncoderPerformance()
    ]);

    const today = dailyStats[0] || {
      videos_encoded: 0,
      total_encoding_time: 0,
      average_encoding_time: 0
    };

    const totalEncoders = encoderPerf.length;
    const activeEncoders = await sqliteManager.getAllEncoders()
      .then(encoders => encoders.filter(e => e.is_active).length);

    const summary = {
      today: {
        videos_encoded: today.videos_encoded,
        total_encoding_time: Math.round(today.total_encoding_time || 0),
        average_encoding_time: Math.round(today.average_encoding_time || 0)
      },
      encoders: {
        total: totalEncoders,
        active: activeEncoders,
        utilization: totalEncoders > 0 ? Math.round((activeEncoders / totalEncoders) * 100) : 0
      },
      performance: {
        jobs_per_hour: encoderPerf.reduce((sum: number, enc: any) => sum + enc.jobs_completed, 0) / 24,
        average_success_rate: encoderPerf.length > 0 
          ? encoderPerf.reduce((sum: number, enc: any) => sum + (enc.success_rate || 0), 0) / encoderPerf.length
          : 0
      }
    };

    const response: ApiResponse = {
      success: true,
      data: summary
    };

    res.json(response);
  } catch (error) {
    logger.error('Error fetching summary statistics', error);
    const response: ApiResponse = {
      success: false,
      error: 'Failed to fetch summary statistics'
    };
    res.status(500).json(response);
  }
});

/**
 * POST /api/statistics/record
 * Record encoder statistics (called by monitoring service)
 */
router.post('/record', async (req, res) => {
  try {
    const { sqliteManager } = getServices();
    const { encoder_id, date, jobs_completed, total_encoding_time } = req.body;
    
    if (!encoder_id || !date || jobs_completed === undefined || total_encoding_time === undefined) {
      const response: ApiResponse = {
        success: false,
        error: 'encoder_id, date, jobs_completed, and total_encoding_time are required'
      };
      return res.status(400).json(response);
    }

    await sqliteManager.recordEncoderStats({
      encoder_id,
      date,
      jobs_completed,
      total_encoding_time
    });

    const response: ApiResponse = {
      success: true,
      message: 'Statistics recorded successfully'
    };

    res.json(response);
  } catch (error) {
    logger.error('Error recording statistics', error);
    const response: ApiResponse = {
      success: false,
      error: 'Failed to record statistics'
    };
    res.status(500).json(response);
  }
});

/**
 * GET /api/statistics/gateway-health
 * Get current gateway health status
 */
router.get('/gateway-health', async (req, res) => {
  try {
    logger.info('Checking gateway health status');
    
    const healthStatus = await getGatewayMonitor().getDetailedHealthStatus();
    
    const response: ApiResponse = {
      success: true,
      data: {
        ...healthStatus,
        gatewayUrl: config.gateway.baseUrl
      }
    };

    res.json(response);
  } catch (error) {
    logger.error('Error getting gateway health status', error);
    const response: ApiResponse = {
      success: false,
      error: 'Failed to get gateway health status',
      data: {
        isOnline: false,
        responseTime: 0,
        lastCheck: new Date(),
        error: 'Health check failed',
        gatewayUrl: config.gateway.baseUrl
      }
    };
    res.status(500).json(response);
  }
});

/**
 * GET /api/statistics/gateway-comprehensive
 * Perform comprehensive gateway test including authentication
 */
router.get('/gateway-comprehensive', async (req, res) => {
  try {
    logger.info('Starting comprehensive gateway health check');
    
    const comprehensiveResult = await getGatewayMonitor().performComprehensiveHealthCheck();
    
    const response: ApiResponse = {
      success: true,
      data: {
        ...comprehensiveResult,
        gatewayUrl: config.gateway.baseUrl,
        timestamp: new Date()
      }
    };

    res.json(response);
  } catch (error) {
    logger.error('Error performing comprehensive gateway check', error);
    const response: ApiResponse = {
      success: false,
      error: 'Failed to perform comprehensive gateway check'
    };
    res.status(500).json(response);
  }
});

/**
 * GET /api/statistics/dashboard
 * Get all dashboard data in one request
 */
router.get('/dashboard', async (req, res) => {
  try {
    const { mongodb, encoderLookup, discordWebhook } = getServices();
    logger.debug('Fetching dashboard data from MongoDB');
    
    // Get all dashboard data in parallel
    const [
      availableJobs,
      jobsInProgress,
      jobsCompletedToday,
      recentJobs,
      activeEncodersCount,
      lastCompletedJobs,
      gatewayHealthStatus
    ] = await Promise.all([
      mongodb.getAvailableJobs(),
      mongodb.getActiveJobs(),
      mongodb.getJobsCompletedToday(),
      mongodb.getRecentJobs(),
      mongodb.getActiveEncodersCount(),
      mongodb.getLastCompletedJobs(10),
      getGatewayMonitor().getDetailedHealthStatus().catch(() => ({ isOnline: false }))
    ]);

    // Calculate Gateway Health
    let gatewayHealth: 'healthy' | 'faulty' | 'dead' = 'healthy';
    
    // First check: Is gateway API responding?
    if (!gatewayHealthStatus.isOnline) {
      gatewayHealth = 'dead';
    } 
    // Second check: If responding, check last completed job
    else if (lastCompletedJobs.length > 0) {
      const mostRecentJob = lastCompletedJobs[0];
      
      if (mostRecentJob.result?.message?.includes('Force processed')) {
        gatewayHealth = 'faulty';
      } else {
        gatewayHealth = 'healthy';
      }
    }
    // If gateway is online but no completed jobs exist, assume healthy
    else {
      gatewayHealth = 'healthy';
    }

    // Helper function to format relative time
    const formatRelativeTime = (date: Date): string => {
      const now = new Date();
      const diffMs = now.getTime() - date.getTime();
      const diffMins = Math.floor(diffMs / (1000 * 60));
      const diffHours = Math.floor(diffMins / 60);
      const diffDays = Math.floor(diffHours / 24);

      if (diffMins < 1) return 'Just now';
      if (diffMins < 60) return `${diffMins}m ago`;
      if (diffHours < 24) return `${diffHours}h ago`;
      return `${diffDays}d ago`;
    };

    // Calculate workload metrics
    const now = new Date();
    const twentyMinutesAgo = new Date(now.getTime() - (20 * 60 * 1000));
    
    // Count active jobs (unassigned, running, assigned)
    const activeJobs = availableJobs.length + jobsInProgress.length;
    
    // Check for old unassigned jobs
    const oldUnassignedJobs = availableJobs.filter(job => 
      new Date(job.created_at) < twentyMinutesAgo
    );
    
    // Calculate workload ratio and zone
    let workloadRatio = activeEncodersCount > 0 ? activeJobs / activeEncodersCount : 0;
    let workloadZone: 'green' | 'yellow' | 'red' = 'green';
    let oldJobsDetected = oldUnassignedJobs.length > 0;
    
    // Auto-red if old jobs and no encoders
    if (oldJobsDetected && activeEncodersCount === 0) {
      workloadZone = 'red';
      workloadRatio = 999; // Indicate critical state
    } else if (workloadRatio >= 5) {
      workloadZone = 'red';
    } else if (workloadRatio >= 3) {
      workloadZone = 'yellow';
    }

    // Get unique encoder DIDs from recent jobs
    const encoderDids = [...new Set(recentJobs
      .map(job => job.assigned_to || job.encoder_id)
      .filter((did): did is string => Boolean(did))
    )];

    // Fetch encoder information for all unique DIDs
    const encoderInfoMap = await encoderLookup.getMultipleEncoderInfos(encoderDids);

    // Format recent jobs for dashboard display with encoder info
    const formattedRecentJobs = recentJobs.map(job => {
      const encoderDid = job.assigned_to || job.encoder_id;
      const encoderInfo = encoderDid ? encoderInfoMap.get(encoderDid) : null;
      
      return {
        id: job.id,
        fullId: job._id || job.id,
        status: job.status,
        videoOwner: job.metadata?.video_owner || job.owner || 'Unknown',
        videoPermlink: job.metadata?.video_permlink || job.permlink || 'unknown',
        videoSize: job.input?.size || job.input_size || 0,
        videoSizeFormatted: formatFileSize(job.input?.size || job.input_size || 0),
        createdAt: job.created_at,
        createdAgo: formatRelativeTime(new Date(job.created_at)),
        assignedTo: encoderDid,
        encoderInfo: encoderInfo ? {
          nodeName: encoderInfo.nodeName,
          hiveAccount: encoderInfo.hiveAccount,
          didKey: encoderInfo.didKey
        } : null,
        progress: job.progress || 0
      };
    });

    const dashboardData = {
      availableJobs: availableJobs.length,
      jobsInProgress: jobsInProgress.length,
      jobsCompletedToday: jobsCompletedToday.length,
      activeEncoders: activeEncodersCount,
      recentJobs: formattedRecentJobs,
      workload: {
        ratio: workloadRatio,
        zone: workloadZone,
        activeJobs: activeJobs,
        activeEncoders: activeEncodersCount,
        oldJobsDetected: oldJobsDetected
      },
      gatewayHealth: gatewayHealth
    };

    // Check and send Discord alerts if needed
    await discordWebhook.checkAndAlert(workloadZone, {
      ratio: workloadRatio,
      activeJobs: activeJobs,
      activeEncoders: activeEncodersCount,
      oldJobsDetected: oldJobsDetected
    });

    logger.info('Dashboard data fetched successfully', {
      availableJobs: dashboardData.availableJobs,
      jobsInProgress: dashboardData.jobsInProgress,
      jobsCompletedToday: dashboardData.jobsCompletedToday,
      activeEncoders: dashboardData.activeEncoders,
      recentJobsCount: dashboardData.recentJobs.length,
      workloadZone: workloadZone,
      workloadRatio: workloadRatio,
      gatewayHealth: gatewayHealth
    });

    const response: ApiResponse = {
      success: true,
      data: dashboardData
    };

    res.json(response);
  } catch (error) {
    logger.error('Error fetching dashboard data', error);
    const response: ApiResponse = {
      success: false,
      error: 'Failed to fetch dashboard data'
    };
    res.status(500).json(response);
  }
});

/**
 * POST /api/statistics/test-discord
 * Test Discord webhook connectivity
 */
router.post('/test-discord', async (req, res) => {
  try {
    const { discordWebhook } = getServices();
    const testResult = await discordWebhook.testWebhook();
    
    const response: ApiResponse = {
      success: testResult,
      data: {
        webhookTested: testResult,
        message: testResult ? 'Discord webhook test successful' : 'Discord webhook test failed - check logs'
      }
    };

    res.json(response);
  } catch (error) {
    logger.error('Error testing Discord webhook', error);
    const response: ApiResponse = {
      success: false,
      error: 'Failed to test Discord webhook'
    };
    res.status(500).json(response);
  }
});

// Helper function to format file sizes
function formatFileSize(bytes: number): string {
  if (bytes === 0) return '0 Bytes';
  
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

export default router;