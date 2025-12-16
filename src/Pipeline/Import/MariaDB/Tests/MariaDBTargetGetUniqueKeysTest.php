<?php

/**
 * This file contains the MariaDBTargetGetUniqueKeysTest class.
 *
 * SPDX-FileCopyrightText: Copyright 2025 Framna Netherlands B.V., Zwolle, The Netherlands
 * SPDX-License-Identifier: MIT
 */

namespace Pipeline\Import\MariaDB\Tests;

use Lunr\Gravity\Tests\Helpers\DatabaseAccessObjectQueryTestTrait;

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
                'keys' => [
                    'column1',
                    'column2',
                ],
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

        $this->class->getUniqueKeys();
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

        $this->expectQueryError();

        $this->db->shouldReceive('get_database')
                 ->once()
                 ->andReturn('database');

        $this->class->getUniqueKeys();
    }

}

?>
