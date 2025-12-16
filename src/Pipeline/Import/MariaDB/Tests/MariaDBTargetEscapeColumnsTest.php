<?php

/**
 * This file contains the MariaDBTargetGetDataTest class.
 *
 * SPDX-FileCopyrightText: Copyright 2025 Framna Netherlands B.V., Zwolle, The Netherlands
 * SPDX-License-Identifier: MIT
 */

namespace Pipeline\Import\MariaDB\Tests;

/**
 * This class contains the tests for the MariaDBTarget.
 *
 * @covers \Pipeline\Import\MariaDB\MariaDBTarget
 */
class MariaDBTargetEscapeColumnsTest extends MariaDBTargetTestCase
{

    /**
     * Test that escapeColumns() escapes columns with a `normal` datatype.
     *
     * @covers \Pipeline\Import\MariaDB\MariaDBTarget::escapeColumns
     */
    public function testEscapeColumnsEscapesColumns(): void
    {
        $this->setReflectionPropertyValue('table', 'table');

        $columns       = [ 'id', 'name' ];
        $returnColumns = [ '`id`', '`name`' ];
        $return        = [[ 'Field' => 'id', 'Type' => 'varchar' ], [ 'Field' => 'name', 'Type' => 'varchar' ]];

        $this->db->shouldReceive('query')
                 ->once()
                 ->andReturn($this->result);

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

        $this->result->shouldReceive('number_of_rows')
                     ->once()
                     ->andReturn(1);

        $this->result->shouldReceive('result_array')
                     ->once()
                     ->andReturn($return);

        $method = $this->getReflectionMethod('escapeColumns');

        $result = $method->invokeArgs($this->class, [ $columns ]);

        $this->assertSame($returnColumns, $result);
    }

    /**
     * Test that escapeColumns() does change the columns if the datatype is a GeoType.
     *
     * @covers \Pipeline\Import\MariaDB\MariaDBTarget::escapeColumns
     */
    public function testFormatColumnsDoesChangeColumnsWithGeoType(): void
    {
        $this->setReflectionPropertyValue('table', 'table');

        $columns = [ 'id', 'coordinates' ];
        $return  = [[ 'Field' => 'id', 'Type' => 'varchar' ], [ 'Field' => 'coordinates', 'Type' => 'POINT' ]];

        $this->db->shouldReceive('query')
                 ->once()
                 ->andReturn($this->result);

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

        $this->result->shouldReceive('number_of_rows')
                     ->once()
                     ->andReturn(count($return));

        $this->result->shouldReceive('result_array')
                     ->once()
                     ->andReturn($return);

        $method = $this->getReflectionMethod('escapeColumns');

        $result = $method->invokeArgs($this->class, [ $columns ]);

        $this->assertSame([ '`id`', 'ST_AsText(`coordinates`) AS `coordinates`' ], $result);
    }

}

?>
