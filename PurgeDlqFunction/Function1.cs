using System;
using System.Threading.Tasks;
using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

namespace PurgeDlqFunction
{
    public class Function1
    {
        [FunctionName("Function1")]
        public static async Task Run([TimerTrigger("0 */5 * * * *")]TimerInfo myTimer, ILogger log)
        {
            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");

            // These next values could be stored in App Settings so it is not hard coded in the app and easily changed
            var topic = "topic1";
            var subscription = "subscription01";
            var sbNamespace = "mnservicebus100";

            string deadLetterQueueName = $"{topic}/Subscriptions/{subscription}/$DeadLetterQueue";

            // Setting up the option to automatically delete the messages as soon as they are retrieved
            var options = new ServiceBusReceiverOptions
            {
                ReceiveMode = ServiceBusReceiveMode.ReceiveAndDelete
            };

            // This setting can be retrieved from the App Settings
            string fullyQualifiedNamespace = $"{sbNamespace}.servicebus.windows.net";

            // Settings for the connection - specifically, the retry settings
            var clientOptions = new ServiceBusClientOptions
            {
                RetryOptions = new ServiceBusRetryOptions
                {
                    Mode = ServiceBusRetryMode.Fixed,
                    MaxRetries = 0,
                    Delay = TimeSpan.FromSeconds(.1),
                    MaxDelay = TimeSpan.FromSeconds(.1)
                }
            };

            // Create a ServiceBusClient that will authenticate through Active Directory using the Function's Managed Identity
            ServiceBusClient client = new(fullyQualifiedNamespace, new DefaultAzureCredential(), clientOptions);

            // Create a receiver to get the messages from the DLQ
            var recv = client.CreateReceiver(deadLetterQueueName, options);

            // Cycle through all messages to clear the DLQ
            while (await recv.PeekMessageAsync() != null)
            {
                try
                {
                    // This command will attempt to retrieve 100 messages from the DLQ
                    await recv.ReceiveMessagesAsync(100);
                }
                catch (Exception ex)
                {
                    // Log the exception here
                    log.LogError(ex.Message);
                }
            }

            // Close the connection
            await recv.CloseAsync();
        }
    }
}
