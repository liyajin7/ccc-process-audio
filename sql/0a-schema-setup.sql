/*
 * Database setup
 */

--
-- Extensions
--

-- PostGIS (from the PostGIS website)
create extension if not exists postgis;
create extension if not exists postgis_raster;
create extension if not exists postgis_topology;

-- PostGIS extensions
create extension if not exists postgis_sfcgal;
create extension if not exists fuzzystrmatch;
create extension if not exists address_standardizer;
create extension if not exists address_standardizer_data_us;
create extension if not exists postgis_tiger_geocoder;

-- Misc extensions
create extension if not exists tablefunc;
create extension if not exists intarray;

--
-- Schemas
--

create schema if not exists dim;
create schema if not exists stg;
create schema if not exists radio;
create schema if not exists twitter;
create schema if not exists election;
create schema if not exists census;
create schema if not exists datasets;

--
-- Functions
--

create or replace function trigger_set_modified_dt()
returns trigger as $$
begin
  new.modified_dt = now();
  return new;
end;
$$ language plpgsql;

--
-- Some common dim data
--

drop table if exists dim.integer cascade;
create table dim.integer
(
    n int primary key
);

drop table if exists dim.date cascade;
create table dim.date
(
    date date primary key
);

drop table if exists dim.state cascade;
create table dim.state
(
    state_id int
        generated by default as identity
        primary key,

    name text not null,
    fips char(2) not null,
    postal_code char(2) not null,
    census_region_5way text not null,

    unique(name) deferrable initially immediate,
    unique(fips) deferrable initially immediate,
    unique(postal_code) deferrable initially immediate
);

begin;
    insert into dim.integer
        (n)
    values
        (0), (1), (2), (3), (4), (5), (6), (7), (8), (9);

    insert into dim.date
        (date)
    select
        '1900-01-01'::date + (x.n::text || ' days')::interval
    from
    (
        select
            i0.n + 10*i1.n + 100*i2.n + 1000*i3.n + 10000*i4.n as n
        from dim.integer i0
            cross join dim.integer i1
            cross join dim.integer i2
            cross join dim.integer i3
            cross join dim.integer i4
    ) x
    where
        '1900-01-01'::date + (x.n::text || ' days')::interval <= '2050-01-01';

    insert into dim.state
        (state_id, name, fips, postal_code, census_region_5way)
    values
        (1, 'Alabama', '01', 'AL', 'South'),
        (2, 'Alaska', '02', 'AK', 'Pacific'),
        (3, 'Arizona', '04', 'AZ', 'West'),
        (4, 'Arkansas', '05', 'AR', 'South'),
        (5, 'California', '06', 'CA', 'West'),
        (6, 'Colorado', '08', 'CO', 'West'),
        (7, 'Connecticut', '09', 'CT', 'Northeast'),
        (8, 'Delaware', '10', 'DE', 'South'),
        (9, 'District of Columbia', '11', 'DC', 'South'),
        (10, 'Florida', '12', 'FL', 'South'),
        (11, 'Georgia', '13', 'GA', 'South'),
        (12, 'Hawaii', '15', 'HI', 'Pacific'),
        (13, 'Idaho', '16', 'ID', 'West'),
        (14, 'Illinois', '17', 'IL', 'Midwest'),
        (15, 'Indiana', '18', 'IN', 'Midwest'),
        (16, 'Iowa', '19', 'IA', 'Midwest'),
        (17, 'Kansas', '20', 'KS', 'Midwest'),
        (18, 'Kentucky', '21', 'KY', 'South'),
        (19, 'Louisiana', '22', 'LA', 'South'),
        (20, 'Maine', '23', 'ME', 'Northeast'),
        (21, 'Maryland', '24', 'MD', 'South'),
        (22, 'Massachusetts', '25', 'MA', 'Northeast'),
        (23, 'Michigan', '26', 'MI', 'Midwest'),
        (24, 'Minnesota', '27', 'MN', 'Midwest'),
        (25, 'Mississippi', '28', 'MS', 'South'),
        (26, 'Missouri', '29', 'MO', 'Midwest'),
        (27, 'Montana', '30', 'MT', 'West'),
        (28, 'Nebraska', '31', 'NE', 'Midwest'),
        (29, 'Nevada', '32', 'NV', 'West'),
        (30, 'New Hampshire', '33', 'NH', 'Northeast'),
        (31, 'New Jersey', '34', 'NJ', 'Northeast'),
        (32, 'New Mexico', '35', 'NM', 'West'),
        (33, 'New York', '36', 'NY', 'Northeast'),
        (34, 'North Carolina', '37', 'NC', 'South'),
        (35, 'North Dakota', '38', 'ND', 'Midwest'),
        (36, 'Ohio', '39', 'OH', 'Midwest'),
        (37, 'Oklahoma', '40', 'OK', 'South'),
        (38, 'Oregon', '41', 'OR', 'West'),
        (39, 'Pennsylvania', '42', 'PA', 'Northeast'),
        (40, 'Rhode Island', '44', 'RI', 'Northeast'),
        (41, 'South Carolina', '45', 'SC', 'South'),
        (42, 'South Dakota', '46', 'SD', 'Midwest'),
        (43, 'Tennessee', '47', 'TN', 'South'),
        (44, 'Texas', '48', 'TX', 'South'),
        (45, 'Utah', '49', 'UT', 'West'),
        (46, 'Vermont', '50', 'VT', 'Northeast'),
        (47, 'Virginia', '51', 'VA', 'South'),
        (48, 'Washington', '53', 'WA', 'West'),
        (49, 'West Virginia', '54', 'WV', 'South'),
        (50, 'Wisconsin', '55', 'WI', 'Midwest'),
        (51, 'Wyoming', '56', 'WY', 'West');
commit;

