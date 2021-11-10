create schema etymology;
create table etymology.vocabulary (
    set_id text,
    word text,
    language text,
    definition text,
    graph text[][2],
    descendants text[],
    upload date
);