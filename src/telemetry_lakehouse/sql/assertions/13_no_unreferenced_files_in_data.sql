-- name: no_unreferenced_files_in_data
-- category: trust
-- description: data/ contains no file that a committed manifest row does not reference (glob readers would trust it).
SELECT relative_path FROM data_dir_files
WHERE relative_path NOT IN (SELECT relative_path FROM m_committed_files)
