namespace FlattenAndPost
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;
    using System.Xml.Serialization;

    using Microsoft.Azure.WebJobs;
    using Microsoft.Azure.WebJobs.Host;
    using Microsoft.Azure.WebJobs.ServiceBus;

    using Newtonsoft.Json;

    public static class FlattenAndPost
    {
        [FunctionName("FlattenAndPost")]
        public static async Task Run(
            [QueueTrigger("smssamples", Connection = "queueConnectionString")]
            string myQueueItem,
            TraceWriter log,
            ExecutionContext context,
            [EventHub("samplesEventHub", Connection = "smssamplesEventHub")]
            IAsyncCollector<string> asyncSampleCollector,
            [EventHub("eventsEventhub", Connection = "smsEventsEventHub")]
            IAsyncCollector<string> asyncEventCollector,
            [Blob("streams/{queueTrigger}", FileAccess.Read)]
            Stream blobStream)
        {
            log.Info($"C# Queue trigger function processed: {myQueueItem}");

            try
            {
                var blobContents = string.Empty;

                using (var streamReader = new StreamReader(blobStream))
                {
                    blobContents = streamReader.ReadToEnd();
                }

                if (blobContents == string.Empty)
                {
                    return;
                }

                var sampleResult = DeserializeResults<MTConnectStreamsType>(blobContents);

                await PostSamples(log, asyncSampleCollector, sampleResult);

                await PostEvents(log, asyncEventCollector, sampleResult);
            }
            catch (Exception e)
            {
                log.Error("Processing samples", e);
            }
        }

        private static T DeserializeResults<T>(string contents)
        {
            T result;

            contents = contents.Trim();

            if (contents == string.Empty)
            {
                return default(T);
            }

            using (var stringReader = new StringReader(contents))
            {
                var serializer = new XmlSerializer(typeof(T));

                result = (T)serializer.Deserialize(stringReader);
            }

            return result;
        }

        private static async Task PostEvents(
            TraceWriter log,
            IAsyncCollector<string> asyncEventCollector,
            MTConnectStreamsType sampleResult)
        {
            var events = default(IEnumerable<EventRecord>);

            if (sampleResult.Streams == null)
            {
                events = new List<EventRecord>();
            }
            else
            {
                var nonEmptyEvents = sampleResult.Streams.Where(
                        s => s.ComponentStream != null
                             && s.ComponentStream.All(cs => cs.Events != null && cs.Events.Any()))
                    .ToList();

                events = nonEmptyEvents.SelectMany(
                    s => s.ComponentStream.SelectMany(
                        cs => cs.Events.Select(
                            e => new EventRecord()
                            {
                                HourWindow =
                                    new DateTime(
                                        e.timestamp.Year,
                                        e.timestamp.Month,
                                        e.timestamp.Day,
                                        e.timestamp.Hour,
                                        0,
                                        0),
                                Id = Guid.NewGuid().ToString(),
                                         DeviceName = s?.name,
                                         DeviceId = s?.uuid,
                                         Component = cs?.component,
                                         ComponentName = cs?.name,
                                         ComponentId = cs?.uuid,
                                         EventDataItemId = e?.dataItemId,
                                         EventTimestamp = e?.timestamp,
                                         EventName = e?.name,
                                         EventSequence = e?.sequence,
                                         EventSubtype = e?.subType,
                                         EventValue = e?.Value
                                     }))).ToList();
            }

            log.Info($"{events.Count()} events found.");
            foreach (var eventRecord in events)
            {
                var flatEvent = JsonConvert.SerializeObject(eventRecord);

                await asyncEventCollector.AddAsync(flatEvent);
            }
        }

        private static async Task PostSamples(
            TraceWriter log,
            IAsyncCollector<string> asyncCollector,
            MTConnectStreamsType sampleResult)
        {
            var samples = default(IEnumerable<SampleRecord>);
            if (sampleResult.Streams == null)
            {
                samples = new List<SampleRecord>();
            }
            else
            {
                var nonEmptySamples = sampleResult.Streams.Where(
                    s => s.ComponentStream != null
                         && s.ComponentStream.All(cs => cs.Samples != null && cs.Samples.Any())).ToList();

                samples = nonEmptySamples.SelectMany(
                    s => s.ComponentStream.SelectMany(
                        cs => cs.Samples.Select(
                            sample => new SampleRecord()
                                          {
                                              HourWindow =
                                                  new DateTime(
                                                      sample.timestamp.Year,
                                                      sample.timestamp.Month,
                                                      sample.timestamp.Day,
                                                      sample.timestamp.Hour,
                                                      0,
                                                      0),
                                              Id = Guid.NewGuid().ToString(),
                                              DeviceName = s?.name,
                                              DeviceId = s?.uuid,
                                              Component = cs?.component,
                                              ComponentName = cs?.name,
                                              ComponentId = cs?.uuid,
                                              SampleDataItemId = sample?.dataItemId,
                                              SampleTimestamp = sample?.timestamp,
                                              SampleName = sample?.name,
                                              SampleSequence = sample?.sequence,
                                              SampleSubtype = sample?.subType,
                                              SampleDuration = sample?.duration,
                                              SampleDurationSpecified = sample?.durationSpecified,
                                              SampleRate = sample?.sampleRate,
                                              SampleStatistic = sample?.statistic,
                                              SampleValue = sample?.Value
                                          }))).ToList();
            }

            log.Info($"{samples.Count()} samples found.");
            foreach (var sample in samples)
            {
                var flatSample = JsonConvert.SerializeObject(sample);

                await asyncCollector.AddAsync(flatSample);
            }
        }
    }

    internal class EventRecord
    {
        public string Component { get; set; }

        public string ComponentId { get; set; }

        public string ComponentName { get; set; }

        public string DeviceId { get; set; }

        public string DeviceName { get; set; }

        public string EventDataItemId { get; set; }

        public string EventName { get; set; }

        public string EventSequence { get; set; }

        public string EventSubtype { get; set; }

        public DateTime? EventTimestamp { get; set; }

        public string EventValue { get; set; }

        public string Id { get; set; }

        public DateTime HourWindow { get; set; }
    }

    internal class SampleRecord
    {
        public SampleRecord()
        {
        }

        public string Component { get; set; }

        public string ComponentId { get; set; }

        public string ComponentName { get; set; }

        public string DeviceId { get; set; }

        public string DeviceName { get; set; }

        public string Id { get; set; }

        public string SampleDataItemId { get; set; }

        public float? SampleDuration { get; set; }

        public bool? SampleDurationSpecified { get; set; }

        public string SampleName { get; set; }

        public string SampleRate { get; set; }

        public string SampleSequence { get; set; }

        public string SampleStatistic { get; set; }

        public string SampleSubtype { get; set; }

        public DateTime? SampleTimestamp { get; set; }

        public string SampleValue { get; set; }

        public DateTime HourWindow { get; set; }
    }
}