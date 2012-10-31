using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Apache.NMS;
using Apache.NMS.ActiveMQ;

namespace IMSKPISender
{
    public class ActiveMQSender
    {
        private readonly string _URI;
        private readonly string _QueueName;
        private readonly Random rdn = new Random();

        public ActiveMQSender(string uri, string queuename)
        {
            _URI = uri;
            _QueueName = queuename;
        }

        public class SendEventOccurred_Args : EventArgs
        {
            public string msg;
            public SendEventOccurred_Args(string msg)
            {
                this.msg = msg;
            }
        }
        public event EventHandler SendEventOccurred;

        public bool SendKpi(string kpiname, string kpivalue, string orgappname, string developername)
        {
            Dictionary<string, string> message = new Dictionary<string, string>();
            try
            {
                message.Add("CLASSNAME", "BusinessSystem");
                message.Add("MAINDATA", "Name=" + orgappname);//需要发送指标所属的系统名称;

                message.Add(kpiname, kpivalue);

                message.Add("TIME", String.Format("{0:yyyy-MM-dd HH:mm:ss}", System.DateTime.Now));
                message.Add("SCENE", developername);

                SendMessage(message);
                message.Clear();
                return true;
            }
            catch
            {
                return false;
            }
        }

        public bool SendOrgappKpis(Dictionary<string, string> kpis, string orgappname, string developername)
        {
            Dictionary<string, string> message = new Dictionary<string, string>();
            try
            {
                message.Add("CLASSNAME", "BusinessSystem");
                message.Add("MAINDATA", "Name=" + orgappname);//需要发送指标所属的系统名称;

                foreach (KeyValuePair<string, string> item in kpis)
                {
                    message.Add(item.Key, item.Value);
                }

                message.Add("TIME", String.Format("{0:yyyy-MM-dd HH:mm:ss}", System.DateTime.Now));
                message.Add("SCENE", developername);
                SendMessage(message);
                message.Clear();
                return true;
            }
            catch
            {
                return false;
            }
        }

        public void SendMessage(string key, string value)
        {
            //Create the Connection Factory
            string msgKey, msgValue;
            using (IConnection connection = new ConnectionFactory(this._URI).CreateConnection())
            {
                using (ISession session = connection.CreateSession())
                {
                    using (IMessageProducer prod = session.CreateProducer(new Apache.NMS.ActiveMQ.Commands.ActiveMQQueue(this._QueueName)))
                    {
                        IMapMessage mapMessage = prod.CreateMapMessage();
                        msgKey = "CLASSNAME";
                        msgValue = "BusinessSystem";
                        mapMessage.Body.SetBytes(msgKey, Encoding.Default.GetBytes(msgValue));

                        msgKey = "MAINDATA";
                        msgValue = "Name=生产管理系统";
                        mapMessage.Body.SetBytes(msgKey, Encoding.Default.GetBytes(msgValue));

                        msgKey = "BusinessSystemOperationHandlingNUM";
                        msgValue = "0";
                        mapMessage.Body.SetBytes(msgKey, Encoding.Default.GetBytes(msgValue));

                        msgKey = "BusinessVisitCount";
                        msgValue = "1234567";
                        mapMessage.Body.SetBytes(msgKey, Encoding.Default.GetBytes(msgValue));

                        msgKey = "BusinessSystemWorkOrderHandlingNUM";
                        msgValue = "0";
                        mapMessage.Body.SetBytes(msgKey, Encoding.Default.GetBytes(msgValue));

                        msgKey = "BusinessSystemDBTime";
                        msgValue = "1.05";
                        mapMessage.Body.SetBytes(msgKey, Encoding.Default.GetBytes(msgValue));

                        msgKey = "BusinessSystemOnlineNum";
                        msgValue = "88";
                        mapMessage.Body.SetBytes(msgKey, Encoding.Default.GetBytes(msgValue));

                        msgKey = "BusinessSystemRunningTime";
                        msgValue = "555555";
                        mapMessage.Body.SetBytes(msgKey, Encoding.Default.GetBytes(msgValue));

                        msgKey = "BusinessDayLoginNum";
                        msgValue = "99";
                        mapMessage.Body.SetBytes(msgKey, Encoding.Default.GetBytes(msgValue));

                        msgKey = "BusinessSystemSessionNum";
                        msgValue = "90";
                        mapMessage.Body.SetBytes(msgKey, Encoding.Default.GetBytes(msgValue));

                        msgKey = "BusinessSystemResponseTime";
                        msgValue = "42";
                        mapMessage.Body.SetBytes(msgKey, Encoding.Default.GetBytes(msgValue));

                        msgKey = "TIME";
                        msgValue = String.Format("{0:yyyy-MM-dd HH:mm:ss}", System.DateTime.Now);
                        mapMessage.Body.SetBytes(msgKey, Encoding.Default.GetBytes(msgValue));

                        msgKey = "SCENE";
                        msgValue = "NARI";
                        mapMessage.Body.SetBytes(msgKey, Encoding.Default.GetBytes(msgValue));

                        prod.Send(mapMessage);
                        if (SendEventOccurred != null)
                        {
                            SendEventOccurred(this, new SendEventOccurred_Args(msgKey + "=" + msgValue + " 已发送."));
                        }
                    }
                }
            }
        }

        private void SendMessage(Dictionary<string, string> message)
        {
            using (IConnection connection = new ConnectionFactory(this._URI).CreateConnection())
            {
                using (ISession session = connection.CreateSession())
                {
                    using (IMessageProducer prod = session.CreateProducer(new Apache.NMS.ActiveMQ.Commands.ActiveMQQueue(this._QueueName)))
                    {
                        IMapMessage mapMessage = prod.CreateMapMessage();
                        foreach (KeyValuePair<string, string> kpi in message)
                        {
                            mapMessage.Body.SetBytes(kpi.Key, Encoding.Default.GetBytes(kpi.Value));
                        }

                        prod.Send(mapMessage);
                        if (SendEventOccurred != null)
                        {
                            SendEventOccurred(this, new SendEventOccurred_Args(" 已发送."));
                        }
                    }
                }
            }
        }
    }
}
