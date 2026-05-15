<?php

/**
 * This file contains the MariaDBTargetGetUniqueKeysTest class.
 *
 * SPDX-FileCopyrightText: Copyright 2025 Framna Netherlands B.V., Zwolle, The Netherlands
 * SPDX-License-Identifier: MIT
 */

namespace Pipeline\Tests\Import\MariaDB;

use Lunr\Gravity\Tests\Helpers\DatabaseAccessObjectQueryTestTrait;
use Pipeline\Import\Exceptions\DatabaseException;

/**
 * This class contains the tests for the MariaDBTarget.
 *
 * @covers \Pipeline\Import\MariaDB\MariaDBTarget
 */
class MariaDBTargetGetUniqueKeysTest extends MariaDBTargetTestCase
{

    use DatabaseAccessObjectQueryTestTrait;

    /**
     * Test that getUniqueKeys() constructs a correct real SQL query.
     *
     * @covers \Pipeline\Import\MariaDB\MariaDBTarget::getUniqueKeys
     */
    public function testGetUniqueKeysConstructsCorrectRealQuery(): void
    {
        $this->setReflectionPropertyValue('table', 'table');

        $content = [
            [
                'name' => 'key_1',
                'keys' => 'column1,column2',
            ],
        ];

        $this->expectResultOnSuccess($content, 'array');

        $this->db->shouldReceive('get_database')
                 ->once()
                 ->andReturn('database');

        $this->class->getUniqueKeys();

        $sql = $this->realBuilder->get_select_query();

        $this->assertSqlStringEqualsSqlFile(TEST_STATICS . '/sql/MariaDBTarget/getUniqueKeys.sql', $sql);
    }

    /**
     * Test that getUniqueKeys() throws an exception if the table name is undefined.
     *
     * @covers \Pipeline\Import\MariaDB\MariaDBTarget::getUniqueKeys
     */
    public function testGetUniqueKeysThrowsExceptionIfTableUndefined(): void
    {
        $this->db->shouldReceive('query')
                 ->never();

        $this->expectException('UnexpectedValueException');
        $this->expectExceptionMessage('No table defined to import to!');

        $this->class->getUniqueKeys();
    }

    /**
     * Test that getUniqueKeys() returns a result set on success.
     *
     * @covers \Pipeline\Import\MariaDB\MariaDBTarget::getUniqueKeys
     */
    public function testGetUniqueKeysReturnsDataOnSuccess(): void
    {
        $this->setReflectionPropertyValue('table', 'table');

        $content = [
            [
                'name' => 'key_1',
                'keys' => 'column1,column2',
            ],
        ];

        $expected = [
            [
                'name' => 'key_1',
                'keys' => [
                    'column1',
                    'column2',
                ],
            ],
        ];

        $this->expectResultOnSuccess($content);

        $this->db->shouldReceive('get_database')
                 ->once()
                 ->andReturn('database');

        $keys = $this->class->getUniqueKeys();

        $this->assertSame($expected, $keys);
    }

    /**
     * Test that getUniqueKeys() returns an empty array if there are no results found.
     *
     * @covers \Pipeline\Import\MariaDB\MariaDBTarget::getUniqueKeys
     */
    public function testGetUniqueKeysReturnsEmptyOnNoResults(): void
    {
        $this->setReflectionPropertyValue('table', 'table');

        $this->expectNoResultsFound();

        $this->db->shouldReceive('get_database')
                 ->once()
                 ->andReturn('database');

        $this->assertArrayEmpty($this->class->getUniqueKeys());
    }

    /**
     * Test that getUniqueKeys() throws an exception if the query failed.
     *
     * @covers \Pipeline\Import\MariaDB\MariaDBTarget::getUniqueKeys
     */
    public function testGetUniqueKeysThrowsExceptionIfQueryFailed(): void
    {
        $this->setReflectionPropertyValue('table', 'table');

        $this->db->shouldReceive('get_new_dml_query_builder_object')
                 ->once()
                 ->with(FALSE)
                 ->andReturn($this->realBuilder);

        $this->db->shouldReceive('query')
                 ->once()
                 ->andReturn($this->result);

        $this->result->shouldReceive('warnings')
                     ->once()
                     ->andReturn(NULL);

        $this->result->shouldReceive('has_failed')
                     ->once()
                     ->andReturn(TRUE);

        $this->result->shouldReceive('error_number')
                     ->once()
                     ->andReturn(1);

        $this->result->shouldReceive('error_message')
                     ->twice()
                     ->andReturn('Error!');

        $this->result->shouldReceive('query')
                     ->twice()
                     ->andReturn('QUERY;');

        $this->result->shouldReceive('has_deadlock')
                     ->zeroOrMoreTimes()
                     ->andReturn(FALSE);

        $this->result->shouldReceive('has_lock_timeout')
                     ->zeroOrMoreTimes()
                     ->andReturn(FALSE);

        $context = [ 'query' => 'QUERY;', 'error' => 'Error!' ];

        $this->logger->expects($this->once())
                     ->method('error')
                     ->with('{query}; failed with error: {error}', $context);

        $this->expectException(DatabaseException::class);
        $this->expectExceptionMessage('Database query error!');

        $this->db->shouldReceive('get_database')
                 ->once()
                 ->andReturn('database');

        $this->class->getUniqueKeys();
    }

}

?>
