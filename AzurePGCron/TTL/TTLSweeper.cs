using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using AzurePGCron.TTLSweeper;
using AzurePGCron;

namespace TTLSweeper
{
    /// <summary>
    /// TTLSweeper class that automatically delete the rows after the expiry interval.
    /// </summary>
    public static class TTLSweeper
    {
        [FunctionName("TTLSweeper")]
        public static void Run([TimerTrigger("0 */5 * * * *")]TimerInfo myTimer, TraceWriter log)
        {
            log.Info($"TTLSweeper function started at: {DateTime.Now}");
            CronConfiguration.LocadConfig();
            Sweeper sweeper = new Sweeper(log);

            try
            {
                sweeper.RunSweeper();
            }
            catch(Exception ex)
            {
                log.Error(ex.ToString());
            }

            log.Info($"TTLSweeper function ended at: {DateTime.Now}");
        }
    }
}
