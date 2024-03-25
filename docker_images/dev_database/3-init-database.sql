\c sandbox;

CREATE TABLE public.clients (
    "id" bigint,
    "address" text,
    "city" text,
    "zip" text,
    "created_at" timestamp
);

CREATE TABLE public.airbnb_places (
    "id" bigint,
    "host_d" bigint,
    "room_type" text,
    "room_price" integer,
    "updated_date" timestamp,
    "city" text,
    "country" text
);