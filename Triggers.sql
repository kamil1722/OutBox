CREATE OR REPLACE FUNCTION main.event_status_change()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
                DECLARE
                    property1_id INT;

                    property1     int4;
                    property2     varchar(50);
                    property3     text;
                BEGIN
				          -- Получаем идентификаторы атрибутов
                    SELECT "Id" INTO property1_id  FROM main.entity_attribute_types WHERE "Code" = 'PROPERTY1' LIMIT 1;

                    -- Срабатываем только если изменяется поле1
                    IF NEW."Property1_Id" <> property1_id THEN
                        RETURN NEW;
                    END IF;

                    -- Если поле не меняется — выходим
                    IF NEW."Value" = OLD."Value" THEN
                        RETURN NEW;
                    END IF;
              
                    -- Постановка для Kafka-outbox
                        SELECT ag."Property1", ag."Property2", ag."Property3"
                        INTO property1, property2, property3
                        FROM main.entitys e
                        JOIN maun.entity_attributes ea on e."Id" = ea."EntityId"
                        WHERE ag."Id" = OLD."EntityId";

                     -- Ключевой механизм upsert который работает благодаря уникальному составному частичному индексу по (EntityId и SentDate IS NULL),
                     -- если для нашей записи существует неотправленное событие(SentDate IS NULL), то ничего не делаем и возвращаем это событие
                        INSERT INTO event.entity_event ("EntityId")
                        VALUES (OLD."EntityId")
                        ON CONFLICT ("EntityId") WHERE "SentDate" IS NULL
                        DO UPDATE SET "EntityId" = EXCLUDED."EntityId"
                        RETURNING "Id" INTO v_event_id;

                        INSERT INTO event.entity_detail
                            ("EventId", "EntityId", "Property1", "Property2", "Property3")
                        VALUES
                            (v_event_id, OLD."EntityId", property1, property2, property3);

                        RETURN NEW;
                END;
                 $function$
;


create trigger trg_status_change before
update
    on
    main.agreement_attributes for each row execute function main.event_status_change()
