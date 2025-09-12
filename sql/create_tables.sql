CREATE TABLE IF NOT EXISTS public.weather_daily (
  event_date date PRIMARY KEY,
  records integer,
  avg_temp_c double precision,
  min_temp_c double precision,
  max_temp_c double precision,
  avg_humidity double precision,
  avg_pressure double precision,
  avg_wind_speed_km_h double precision
);
