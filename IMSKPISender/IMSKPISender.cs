using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using System.Configuration;
using System.Timers;
using System.Data.Common;

namespace IMSKPISender
{
    public partial class IMSKPISendService : ServiceBase
    {
        private string _DBConnStr = "";
        private string _MQServerUri = "";
        private string _QueueName = "";
        private int _Interval = 1;
        private int _KpiInterval = 5;
        private Timer _Timer;
        private DbProviderFactory _df;
        private const string _logName = "IMS KPI Send Service";
        private const string _eventSourceName = "IMSKPISendService";

        public IMSKPISendService()
        {
            InitializeComponent();
            _df = DbProviderFactories.GetFactory("System.Data.SqlClient");
        }

        protected override void OnStart(string[] args)
        {
            if (Init())
            {
                Log(_eventSourceName + " Started!");
            }
            else
            {
                LogError("Init error.");
            }
        }

        private bool Init()
        {
            try
            {
                if (!EventLog.SourceExists(_eventSourceName))
                {
                    var eventSourceData = new EventSourceCreationData(_eventSourceName, _logName);
                    EventLog.CreateEventSource(eventSourceData);
                }

                _DBConnStr = ConfigurationManager.AppSettings["DBConnStr"].Trim();
                _MQServerUri = ConfigurationManager.AppSettings["MQServerUri"].Trim();
                _QueueName = ConfigurationManager.AppSettings["QueueName"].Trim();
                _Interval = int.Parse(ConfigurationManager.AppSettings["Interval"].Trim());
                _KpiInterval = int.Parse(ConfigurationManager.AppSettings["KpiInterval"].Trim());

                _Timer = new Timer() { Interval = _Interval, AutoReset = true, Enabled = true };
                _Timer.Elapsed += new ElapsedEventHandler(SendKpi);

                Log("Init completed.");
                return true;
            }
            catch(Exception ex)
            {
                LogError(ex.Message);
                return false;
            }
        }

        private DateTime Get5Minute0(DateTime t)
        {
            decimal m0 = t.Minute;
            decimal m = System.Math.Ceiling(m0 / this._KpiInterval);
            return new DateTime(t.Year, t.Month, t.Day, t.Hour, (int)m, 0);
        }
        private DateTime Get5Minute1(DateTime t)
        {
            decimal m0 = t.Minute;
            decimal m = System.Math.Floor(m0 / this._KpiInterval);
            return new DateTime(t.Year, t.Month, t.Day, t.Hour, (int)m, 0);
        }

        private void SendKpi(object source,ElapsedEventArgs e)
        {
            List<int> sendedKpiIds = new List<int>();

            try
            {
                ActiveMQSender sender = new ActiveMQSender(this._MQServerUri, this._QueueName);
                using (DbConnection cn = _df.CreateConnection())
                {
                    cn.ConnectionString = _DBConnStr;
                    cn.Open();

                    DbCommand cmdCheck = _df.CreateCommand();
                    cmdCheck.Connection = cn;
                    DateTime now = DateTime.Now;
                    cmdCheck.CommandText=string.Format("Select * From [SendData] where [SendTime]>='{0}' and [SendTime]<'{1}'",
                         Get5Minute0(now), Get5Minute1(now));
                    using (DbDataReader dr = cmdCheck.ExecuteReader())
                    {
                        while (dr.Read())
                        {
                            if(sender.SendKpi(dr["KpiName"].ToString(), dr["KpiValue"].ToString(), dr["OrgappName"].ToString(), dr["DeveloperName"].ToString()))
                                sendedKpiIds.Add(int.Parse(dr["Id"].ToString()));
                        }
                    }

                    DbCommand cmdDelete = _df.CreateCommand();
                    cmdDelete.Connection = cn;
                    foreach (int id in sendedKpiIds)
                    {
                        cmdDelete.CommandText = "Delete From [SendData] Where [Id]=" + id;
                        cmdDelete.ExecuteNonQuery();
                    }


                    cmdDelete.Dispose();
                    cmdCheck.Dispose();
                    cn.Close();
                }

            }
            catch(Exception ex)
            {
                LogError(ex.Message);
            }
        }

        private void Log(string msg)
        {
            using (var log = new EventLog(_logName, ".", _eventSourceName))
            {
                log.WriteEntry(msg);
            }
        }
        private void LogWarning(string msg)
        {
            using (var log = new EventLog(_logName, ".", _eventSourceName))
            {
                log.WriteEntry(msg,EventLogEntryType.Warning);
            }
        }
        private void LogError(string msg)
        {
            using (var log = new EventLog(_logName, ".", _eventSourceName))
            {
                log.WriteEntry(msg,EventLogEntryType.Error);
            }
        }

        protected override void OnStop()
        {
            Log(_eventSourceName + " Stoped!");
        }
    }
}
