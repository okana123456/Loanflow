-- Read only. Path characters are masked; no filenames or customer details.
SELECT
  regexp_replace(name,'[A-Za-z0-9]','x','g') AS masked_path_shape,
  array_length(string_to_array(name,'/'),1) AS path_segments,
  lower(substring(name from '[.][^.]+$')) AS file_extension,
  count(*) AS files,
  min(created_at) AS earliest_upload,
  max(created_at) AS latest_upload
FROM storage.objects
WHERE bucket_id='client-photos'
  AND split_part(name,'/',1)='BIZ-B3F5E5D9'
GROUP BY regexp_replace(name,'[A-Za-z0-9]','x','g'),
  array_length(string_to_array(name,'/'),1),
  lower(substring(name from '[.][^.]+$'))
ORDER BY files DESC;
