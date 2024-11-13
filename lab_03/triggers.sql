--Триггер AFTER
--DROP FUNCTION IF EXISTS t.after_insert_client() CASCADE;
CREATE OR REPLACE FUNCTION t.after_insert_client() 
RETURNS TRIGGER AS $$
BEGIN
    RAISE NOTICE 'New client added: %', NEW.name || ' ' || NEW.surname;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

--DROP TRIGGER IF EXISTS after_client_insert ON t.client;
CREATE OR REPLACE TRIGGER after_client_insert
AFTER INSERT ON t.client
FOR EACH ROW EXECUTE FUNCTION t.after_insert_client();

--Триггер INSTEAD OF (INSERT, UPDATE или DELETE)
CREATE OR REPLACE VIEW t.client_view AS
SELECT * FROM t.client;

CREATE OR REPLACE FUNCTION t.instead_of_update_client() 
RETURNS TRIGGER AS $$
BEGIN
	RAISE NOTICE 'Update client: %', NEW.name || ' ' || NEW.surname;
    UPDATE t.client
	SET name = NEW.name, surname = NEW.surname 
   	WHERE id = OLD.id;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

--DROP TRIGGER IF EXISTS instead_of_client_update ON t.client;
CREATE OR REPLACE TRIGGER instead_of_client_update
INSTEAD OF UPDATE ON t.client_view
FOR EACH ROW EXECUTE FUNCTION t.instead_of_update_client();



