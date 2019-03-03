using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using AzurePGCron.VacuumSweeper;

namespace AzurePGCron
{
    /// <summary>
    /// VacuumCron function that sets auto vacuum configuration and run manual vacuum if required.
    /// </summary>
    public static class VacuumCron
    {
        [FunctionName("VacuumCron")]
        public static void Run([TimerTrigger("0 */5 * * * *")]TimerInfo myTimer, TraceWriter log)
        {
            log.Info($"VacuumCron Timer trigger function started at: {DateTime.Now}");
            CronConfiguration.LocadConfig();
            Vacuum v = new Vacuum(log);
            v.RunVacuum();
            log.Info($"VacuumCron Timer trigger function ended at: {DateTime.Now}");
        }
    }
}
