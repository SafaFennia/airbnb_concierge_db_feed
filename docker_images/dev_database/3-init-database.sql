
CREATE ROLE postgres;

CREATE FUNCTION update_edited_column() RETURNS trigger AS $$
    BEGIN
        NEW.edited := True;
        RETURN NEW;
    END;
$$ LANGUAGE 'plpgsql';

CREATE TABLE public.clients (
    "id" bigint,
    "address" text,
    "city" text,
    "zip" text,
    "created_at" datetime,
);

CREATE TABLE public.airbnb_places (
    "id" bigint,
    "host_d" bigint,
    "room_type" text,
    "room_price" integer

    "updated_date" datetime,
    "city" text,
    "country" text
);