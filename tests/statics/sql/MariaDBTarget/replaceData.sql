INSERT INTO `table` (`id`, `language`, `revision`, `name`, `valid`)
VALUES ('a1', 'en-US', 3, 'yeah', 0)
ON DUPLICATE KEY UPDATE `id` = VALUES (`id`),
`language` = VALUES (`language`),
`revision` = VALUES (`revision`),
`name` = VALUES (`name`),
`valid` = VALUES (`valid`)
