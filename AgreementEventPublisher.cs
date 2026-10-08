using System.Text;
using System.Text.Json;
using AgreementService.Adapters.Db.Context;
using AgreementService.Domain.Config;
using AgreementService.Domain.Entities;
using AgreementService.Domain.Models.EventModels;
using AgreementService.Domain.Ports;
using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace AgreementService.Event.Background
{
    public class AgreementEventPublisher : BackgroundService
    {
        private readonly int _batchSize;
        private readonly int _readDelay;
        private readonly string _topicName;

        private readonly IDbContextFactory<AgreementDbContext> _contextFactory;
        private readonly IProducer<string, byte[]> _producer;
        private readonly ILogPort<AgreementEventPublisher> _logger;

        public AgreementEventPublisher(
            IDbContextFactory<AgreementDbContext> contextFactory,
            IProducer<string, byte[]> producer,
            IOptions<KafkaConfig> kafkaOptions,
            IOptions<ListenerConfig> listenerOptions,
            ILogPort<AgreementEventPublisher> logger)
        {
            _contextFactory = contextFactory;
            _producer = producer;
            _topicName = kafkaOptions.Value.ConsentEventsTopic;
            _batchSize = listenerOptions.Value.BatcthSize;
            _readDelay = listenerOptions.Value.ReadDelay;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                int processed;
                try
                {
                    processed = await ProcessBatchAsync(stoppingToken);
                }
                catch (Exception ex)
                {
                    await _logger.LogError(ex, "Ошибка обработки батча событий");
                    processed = 0;
                }

                if (processed == 0)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(_readDelay), stoppingToken);
                }
            }
        }

        private async Task<int> ProcessBatchAsync(CancellationToken ct)
        {
            //Транзакция обязательна для обеспечения атомарности запросов
            await using var db = await _contextFactory.CreateDbContextAsync(ct);
            await using var tx = await db.Database.BeginTransactionAsync(ct);

            //FOR UPDATE SKIP LOCKED пропускаем занятый набор строк  в таблице по фильтру WHERE "SentDate" IS NULL для предотвращения гонки подов. 
            //например если BatchSize = 100 , Под 1 - берет 1-100 значений, Под 2- 101-200 включительно и тд
            var events = await db.Database.SqlQuery<AgreementEventRow>($"""
                SELECT "Id", "AgreementId"
                FROM event.agreements_event
                WHERE "SentDate" IS NULL
                ORDER BY "Id"
                FOR UPDATE SKIP LOCKED
                LIMIT {_batchSize}
            """).ToListAsync(ct);

            if (events.Count == 0)
            {
                await tx.CommitAsync(ct);
                return 0;
            }

            await _logger.LogInformation(
                "Получено {Count} событий для обработки",
                events.Count);

            var eventIds = events.Select(e => e.Id).ToArray();

            //Получаем набор строк для детализации событий
            var details = await db.Set<AgreementDetail>()
                .Include(e => e.EventType)
                .Where(d => eventIds.Contains(d.EventId))
                .OrderBy(d => d.AgreementId)
                .ThenBy(d => d.CreateDate)
                .ToListAsync(ct);

            var deliveries = new List<Task<DeliveryResult<string, byte[]>>>(details.Count);
            //Формируем бтач
            foreach (var detail in details)
            {
                var payload = new AgreementEventMessage(
                    detail.AgreementId,
                    detail.SubjectId,
                    detail.Snils,
                    detail.BirthDate,
                    detail.AgreementTypeCode,
                    detail.Status);

                var kafkaMessage = new Message<string, byte[]>
                {
                    Key = detail.SubjectId.ToString(),
                    Value = JsonSerializer.SerializeToUtf8Bytes(payload),
                    Headers = new Headers
                    {
                        { "source", Encoding.UTF8.GetBytes("agreement-service") },
                        { "event-id", Encoding.UTF8.GetBytes(detail.EventId.ToString()) },
                        { "event-type", Encoding.UTF8.GetBytes(detail.EventType.Code.ToString()) },
                        { "created-at", Encoding.UTF8.GetBytes(detail.CreateDate.ToString("O")) }
                    }
                };

                deliveries.Add(_producer.ProduceAsync(_topicName, kafkaMessage, ct));
            }

            await _logger.LogInformation(
                "Отправка {Count} сообщений в Kafka, топик = {Topic}",
                deliveries.Count,
                _topicName);

            try
            {
                //Отправляем батч
                await Task.WhenAll(deliveries);
                await _logger.LogInformation(
                    "Успешная отправка {Count} сообщений в Kafka",
                    deliveries.Count);
            }
            catch (Exception ex)
            {
                await _logger.LogError(
                    ex,
                    "Ошибка Kafka при отправке батча из {Count} событий, откат транзакции",
                    events.Count);

                await tx.RollbackAsync(ct);
                return 0;
            }

            var now = DateTimeOffset.UtcNow;
            //Помечаем событие как отправленное
            await db.Database.ExecuteSqlInterpolatedAsync(
                $"""
                UPDATE event.agreements_event
                SET "SentDate" = {now}
                WHERE "Id" = ANY({eventIds})
                """,
                ct);

            await tx.CommitAsync(ct);

            await _logger.LogInformation(
                "Батч обработан: событий = {Events}, деталей = {Details}",
                events.Count,
                details.Count);

            return events.Count;
        }
    }

    public record struct AgreementEventMessage(
        int AgreementId,
        int SubjectId,
        string? Snils,
        DateOnly? BirthDate,
        string AgreementTypeCode,
        string Status);
}
