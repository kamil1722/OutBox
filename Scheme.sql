-- entity_detail - Детализация события
create table "event".entity_detail ( "Id" serial4 not null,
"EventId" int4 not null,
"EntityId" int4 not null,
"Property1" int4 not null,
"Property2" varchar(20) not null,
"Property3" varchar(20) not null,
constraint entity_detail_pkey primary key ("Id"));

--entity_event - События изменения/добавления
create table "event".entity_event ( "Id" serial4 not null,
"EntityId" int4 not null,
"SentDate" timestamptz null,
constraint entity_event_pkey primary key ("Id"));
-- индексы
create index "IX_event_entity_SentDate" on
event.entity_event using btree ("SentDate");
--уникальный составной частичный индекс - предотвращает запись двух событий одной сущности если SentDate одного из событий равен null
create unique index "UX_event_entity_EntityId_Pending" on
event.entity_event using btree ("EntityId")
where ("SentDate" is null);
-- "event".entity_event внешние включи
alter table "event".entity_event add constraint "FK_entity_event_entity_EntityId" foreign key ("EntityId") references main.entity("Id") on
delete
    cascade;
