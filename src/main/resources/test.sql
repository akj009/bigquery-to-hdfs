SELECT
    *
FROM
    `test-project.test_dataset.test_table`
WHERE
    _PARTITIONTIME = %partition_time AND epoch BETWEEN %start_time AND %end_time