<?php

/**
 * This file contains the MariaDBTargetGetDataTest class.
 *
 * SPDX-FileCopyrightText: Copyright 2025 Framna Netherlands B.V., Zwolle, The Netherlands
 * SPDX-License-Identifier: MIT
 */

namespace Pipeline\Import\MariaDB\Tests;

use Lunr\Gravity\Tests\Helpers\DatabaseAccessObjectQueryTestTrait;
use Pipeline\Import\ContentRangeInterface;

/**
 * This class contains the tests for the MariaDBTarget.
 *
 * @covers \Pipeline\Import\MariaDB\MariaDBTarget
 */
class MariaDBTargetGetDataTest extends MariaDBTargetTestCase
{

    use DatabaseAccessObjectQueryTestTrait;

    /**
     * Test that getData() constructs the query correctly.
     *
     * @covers \Pipeline\Import\MariaDB\MariaDBTarget::getData
     */
    public function testGetDataConstructsQuery(): void
    {
        $this->setReflectionPropertyValue('table', 'table');

        $content = [[ 'hello' => 'value', 'world' => 'value' ]];

        $this->expectResultOnSuccess($content, 'array');

        $this->class->getData();

        $sql = $this->realBuilder->get_select_query();

        $this->assertSqlStringEqualsSqlFile(TEST_STATICS . '/sql/MariaDBTarget/getData_no_columns.sql', $sql);
    }

    /**
     * Test that getData() constructs the query correctly.
     *
     * @covers \Pipeline\Import\MariaDB\MariaDBTarget::getData
     */
    public function testGetDataConstructsQueryWithColumns(): void
    {
        $this->setReflectionPropertyValue('table', 'table');

        $data    = [ 'hello', 'world' ];
        $content = [[ 'hello' => 'value', 'world' => 'value' ]];

        $this->db->shouldReceive('get_new_dml_query_builder_object')
                 ->once()
                 ->andReturn($this->realBuilder);

        $this->db->shouldReceive('query')
                 ->twice()
                 ->andReturn($this->result);

        $this->result->shouldReceive('warnings')
                     ->twice()
                     ->andReturn(NULL);

        $this->result->shouldReceive('has_failed')
                     ->twice()
                     ->andReturn(FALSE);

        $this->result->shouldReceive('has_deadlock')
                     ->zeroOrMoreTimes()
                     ->andReturn(FALSE);

        $this->result->shouldReceive('has_lock_timeout')
                     ->zeroOrMoreTimes()
                     ->andReturn(FALSE);

        $this->result->shouldReceive('number_of_rows')
                     ->once()
                     ->andReturn(2);

        $this->result->shouldReceive('number_of_rows')
                     ->once()
                     ->andReturn(1);

        $this->result->shouldReceive('result_array')
                     ->once()
                     ->andReturn(
                         [
                             [
                                 'Field' => 'hello',
                                 'Type' => 'varchar'
                             ],
                             [
                                 'Field' => 'world',
                                 'Type' => 'varchar'
                             ]
                         ]
                    );

        $this->result->shouldReceive('result_array')
                     ->once()
                     ->andReturn($content);

        $this->class->getData($data);

        $sql = $this->realBuilder->get_select_query();

        $this->assertSqlStringEqualsSqlFile(TEST_STATICS . '/sql/MariaDBTarget/getData_columns.sql', $sql);
    }

    /**
     * Test that getData() constructs the query correctly.
     *
     * @covers \Pipeline\Import\MariaDB\MariaDBTarget::getData
     */
    public function testGetDataConstructsQueryWithRange(): void
    {
        $this->setReflectionPropertyValue('table', 'table');

        $content = [[ 'hello' => 'value', 'world' => 'value' ]];

        $this->expectResultOnSuccess($content, 'array');

        $range1 = $this->getMockBuilder(ContentRangeInterface::class)
                       ->getMock();

        $range2 = $this->getMockBuilder(ContentRangeInterface::class)
                       ->getMock();

        $ranges = [ $range1, $range2 ];

        $range1->expects($this->once())
               ->method('apply')
               ->willReturnCallback(function () {
                   $builder = $this->class->getQueryBuilder();
                   $escaper = $this->class->getQueryEscaper();

                   $builder->where($escaper->column('range1'), $escaper->value('value1'));
               });

        $range2->expects($this->once())
               ->method('apply')
               ->willReturnCallback(function () {
                   $builder = $this->class->getQueryBuilder();
                   $escaper = $this->class->getQueryEscaper();

                   $builder->where($escaper->column('range2'), $escaper->value('value2'));
               });

        $this->class->getData(ranges: $ranges);

        $sql = $this->realBuilder->get_select_query();

        $this->assertSqlStringEqualsSqlFile(TEST_STATICS . '/sql/MariaDBTarget/getData_with_range.sql', $sql);
    }

    /**
     * Test that getData() constructs the query correctly.
     *
     * @covers \Pipeline\Import\MariaDB\MariaDBTarget::getData
     */
    public function testGetDataConstructsQueryWithEmptyRange(): void
    {
        $this->setReflectionPropertyValue('table', 'table');

        $content = [[ 'hello' => 'value', 'world' => 'value' ]];

        $this->expectResultOnSuccess($content, 'array');

        $range1 = $this->getMockBuilder(ContentRangeInterface::class)
                       ->getMock();

        $range2 = $this->getMockBuilder(ContentRangeInterface::class)
                       ->getMock();

        $ranges = [ $range1, $range2 ];

        $range1->expects($this->once())
               ->method('isEmpty')
               ->willReturn(TRUE);

        $range2->expects($this->once())
               ->method('isEmpty')
               ->willReturn(TRUE);

        $this->class->getData(ranges: $ranges);

        $sql = $this->realBuilder->get_select_query();

        $this->assertSqlStringEqualsSqlFile(TEST_STATICS . '/sql/MariaDBTarget/getData_no_columns.sql', $sql);
    }

    /**
     * Test that getData() returns data when successful.
     *
     * @covers \Pipeline\Import\MariaDB\MariaDBTarget::getData
     */
    public function testGetDataReturnsDataOnSuccess(): void
    {
        $this->setReflectionPropertyValue('table', 'table');

        $data   = [ 'hello', 'world' ];
        $format = 'array';

        $content = [[ 'hello' => 'value' ]];

        $this->db->shouldReceive('get_new_dml_query_builder_object')
                 ->once()
                 ->andReturn($this->realBuilder);

        $this->db->shouldReceive('query')
                 ->twice()
                 ->andReturn($this->result);

        $this->result->shouldReceive('warnings')
                     ->twice()
                     ->andReturn(NULL);

        $this->result->shouldReceive('has_failed')
                     ->twice()
                     ->andReturn(FALSE);

        $this->result->shouldReceive('has_deadlock')
                     ->zeroOrMoreTimes()
                     ->andReturn(FALSE);

        $this->result->shouldReceive('has_lock_timeout')
                     ->zeroOrMoreTimes()
                     ->andReturn(FALSE);

        $this->result->shouldReceive('number_of_rows')
                     ->twice()
                     ->andReturn(1);

        $this->result->shouldReceive('result_' . $format)
                     ->once()
                     ->andReturn([[ 'Field' => 'column', 'Type' => 'type' ]]);

        $this->result->shouldReceive('result_' . $format)
                     ->once()
                     ->andReturn($content);

        $this->assertSame($content, $this->class->getData($data));
    }

    /**
     * Test that getData() returns an empty array when nothing is found.
     *
     * @covers \Pipeline\Import\MariaDB\MariaDBTarget::getData
     */
    public function testGetDataReturnsEmptyOnNoResults(): void
    {
        $this->setReflectionPropertyValue('table', 'table');

        $this->db->shouldReceive('get_new_dml_query_builder_object')
                 ->once()
                 ->andReturn($this->realBuilder);

        $this->db->shouldReceive('query')
                 ->twice()
                 ->andReturn($this->result);

        $this->result->shouldReceive('warnings')
                     ->twice()
                     ->andReturn(NULL);

        $this->result->shouldReceive('has_failed')
                     ->twice()
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

        $this->result->shouldReceive('number_of_rows')
                     ->once()
                     ->andReturn(0);

        $this->result->shouldReceive('result_array')
                     ->once()
                     ->andReturn([[ 'Field' => 'column', 'Type' => 'type' ]]);

        $data = [ 'hello', 'world' ];

        $this->assertSame([], $this->class->getData($data));
    }

    /**
     * Test that getData() throws an exception on failures.
     *
     * @covers \Pipeline\Import\MariaDB\MariaDBTarget::getData
     */
    public function testGetDataThrowsExceptionOnFailure(): void
    {
        $this->setReflectionPropertyValue('table', 'table');

        $this->expectQueryError();

        $data = [ 'hello', 'world' ];

        $this->class->getData($data);
    }

    /**
     * Test that getData() throws an exception when the table name is undefined.
     *
     * @covers \Pipeline\Import\MariaDB\MariaDBTarget::getData
     */
    public function testGetDataThrowsExceptionIfTableUndefined(): void
    {
        $this->db->shouldReceive('query')
                 ->never();

        $data = [ 'hello', 'world' ];

        $this->expectException('UnexpectedValueException');
        $this->expectExceptionMessage('No table defined to import to!');

        $this->class->getData($data);
    }

}

?>
