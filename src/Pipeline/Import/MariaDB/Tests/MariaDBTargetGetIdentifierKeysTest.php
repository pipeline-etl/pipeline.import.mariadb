<?php

/**
 * This file contains the MariaDBTargetGetIdentifierKeysTest class.
 *
 * SPDX-FileCopyrightText: Copyright 2025 Framna Netherlands B.V., Zwolle, The Netherlands
 * SPDX-License-Identifier: MIT
 */

namespace Pipeline\Import\MariaDB\Tests;

use Lunr\Gravity\Exceptions\QueryException;
use Lunr\Gravity\Tests\Helpers\DatabaseAccessObjectQueryTestTrait;

/**
 * This class contains the tests for the MariaDBTarget.
 *
 * @covers \Pipeline\Import\MariaDB\MariaDBTarget
 */
class MariaDBTargetGetIdentifierKeysTest extends MariaDBTargetTestCase
{

    use DatabaseAccessObjectQueryTestTrait;

    /**
     * Test that getIdentifierKeys() constructs a correct SQL query.
     *
     * @covers \Pipeline\Import\MariaDB\MariaDBTarget::getIdentifierKeys
     */
    public function testGetIdentifierKeysConstructsCorrectQuery(): void
    {
        $this->setReflectionPropertyValue('table', 'table');

        $this->db->shouldReceive('get_new_dml_query_builder_object')
                 ->never();

        $this->db->shouldReceive('query')
                 ->once()
                 ->with('SHOW COLUMNS FROM table WHERE `Key` = \'PRI\'')
                 ->andReturn($this->result);

        $this->result->shouldReceive('number_of_rows')
                     ->once()
                     ->andReturn(1);

        $this->result->shouldReceive('warnings')
                     ->once()
                     ->andReturn(NULL);

        $this->result->shouldReceive('has_failed')
                     ->once()
                     ->andReturn(FALSE);

        $this->result->shouldReceive('has_deadlock')
                     ->zeroOrMoreTimes()
                     ->andReturn(FALSE);

        $this->result->shouldReceive('has_lock_timeout')
                     ->zeroOrMoreTimes()
                     ->andReturn(FALSE);

        $this->result->shouldReceive('result_column')
                     ->once()
                     ->andReturn([ 'hello', 'world' ]);

        $this->class->getIdentifierKeys();
    }

    /**
     * Test that getIdentifierKeys() throws an exception if the table name is undefined.
     *
     * @covers \Pipeline\Import\MariaDB\MariaDBTarget::getIdentifierKeys
     */
    public function testGetIdentifierKeysThrowsExceptionIfTableUndefined(): void
    {
        $this->db->shouldReceive('query')
                 ->never();

        $this->expectException('UnexpectedValueException');
        $this->expectExceptionMessage('No table defined to import to!');

        $this->class->getIdentifierKeys();
    }

    /**
     * Test that getIdentifierKeys() throws an exception if the query failed.
     *
     * @covers \Pipeline\Import\MariaDB\MariaDBTarget::getIdentifierKeys
     */
    public function testGetIdentifierKeysThrowsExceptionIfQueryFailed(): void
    {
        $this->setReflectionPropertyValue('table', 'table');

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
                     ->andReturn('QUERY');

        $this->result->shouldReceive('has_deadlock')
                     ->zeroOrMoreTimes()
                     ->andReturn(FALSE);

        $this->result->shouldReceive('has_lock_timeout')
                     ->zeroOrMoreTimes()
                     ->andReturn(FALSE);

        $context = [ 'query' => 'QUERY', 'error' => 'Error!' ];

        $this->logger->expects($this->once())
                     ->method('error')
                     ->with('{query}; failed with error: {error}', $context);

        $this->expectException(QueryException::class);
        $this->expectExceptionMessage('Database query error!');

        $this->class->getIdentifierKeys();
    }

    /**
     * Test that getData() returns data when successful.
     *
     * @covers \Pipeline\Import\MariaDB\MariaDBTarget::getIdentifierKeys
     */
    public function testGetIdentifierKeysReturnsDataOnSuccess(): void
    {
        $this->setReflectionPropertyValue('table', 'table');

        $this->db->shouldReceive('query')
                 ->once()
                 ->andReturn($this->result);

        $this->result->shouldReceive('number_of_rows')
                     ->once()
                     ->andReturn(1);

        $this->result->shouldReceive('warnings')
                     ->once()
                     ->andReturn(NULL);

        $this->result->shouldReceive('has_failed')
                     ->once()
                     ->andReturn(FALSE);

        $this->result->shouldReceive('has_deadlock')
                     ->zeroOrMoreTimes()
                     ->andReturn(FALSE);

        $this->result->shouldReceive('has_lock_timeout')
                     ->zeroOrMoreTimes()
                     ->andReturn(FALSE);

        $this->result->shouldReceive('result_column')
                     ->once()
                     ->andReturn([ 'hello', 'world' ]);

        $this->assertEquals([ 'hello', 'world' ], $this->class->getIdentifierKeys());
    }

    /**
     * Test that getIdentifierKeys() caches the returned values in a property.
     *
     * @covers \Pipeline\Import\MariaDB\MariaDBTarget::getIdentifierKeys
     */
    public function testGetIdentifierKeysSetsPropertyValue(): void
    {
        $this->setReflectionPropertyValue('table', 'table');

        $this->db->shouldReceive('query')
                 ->once()
                 ->andReturn($this->result);

        $this->result->shouldReceive('number_of_rows')
                     ->once()
                     ->andReturn(1);

        $this->result->shouldReceive('warnings')
                     ->once()
                     ->andReturn(NULL);

        $this->result->shouldReceive('has_failed')
                     ->once()
                     ->andReturn(FALSE);

        $this->result->shouldReceive('has_deadlock')
                     ->zeroOrMoreTimes()
                     ->andReturn(FALSE);

        $this->result->shouldReceive('has_lock_timeout')
                     ->zeroOrMoreTimes()
                     ->andReturn(FALSE);

        $this->result->shouldReceive('result_column')
                     ->once()
                     ->andReturn([ 'hello', 'world' ]);

        $this->class->getIdentifierKeys();

        $this->assertPropertyEquals('identifierKeys', [ 'hello', 'world' ]);
    }

}

?>
