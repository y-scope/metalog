-- Development credentials for the local Compose stack only. This bootstrap runs
-- on every stack start so existing MariaDB volumes acquire the required users.
-- Recreate only these dedicated development accounts to reset prior grants,
-- role memberships and default roles before applying the intended privileges.
DROP USER IF EXISTS 'metalog_writer'@'%';
CREATE USER 'metalog_writer'@'%' IDENTIFIED BY 'metalog_writer_dev';
GRANT ALL PRIVILEGES ON metalog_metastore.* TO 'metalog_writer'@'%';

DROP USER IF EXISTS 'metalog_reader'@'%';
CREATE USER 'metalog_reader'@'%' IDENTIFIED BY 'metalog_reader_dev';
GRANT SELECT ON metalog_metastore.* TO 'metalog_reader'@'%';

DROP USER IF EXISTS 'metalog_monitor'@'%';
CREATE USER 'metalog_monitor'@'%' IDENTIFIED BY 'metalog_monitor_dev';
GRANT USAGE ON *.* TO 'metalog_monitor'@'%';
