-- create pet table
CREATE TABLE IF NOT EXISTS pet (
    pet_id SERIAL PRIMARY KEY,
    name VARCHAR NOT NULL,
    pet_type VARCHAR NOT NULL,
    birth_date DATE NOT NULL,
    OWNER VARCHAR NOT NULL);
-- populate pet table
-- INSERT INTO pet VALUES ( 'Max', 'Dog', '2018-07-05', 'Jane');
-- INSERT INTO pet VALUES ( 'Susie', 'Cat', '2019-05-01', 'Phil');
-- INSERT INTO pet VALUES ( 'Lester', 'Hamster', '2020-06-23', 'Lily');
-- INSERT INTO pet VALUES ( 'Quincy', 'Parrot', '2013-08-11', 'Anne');