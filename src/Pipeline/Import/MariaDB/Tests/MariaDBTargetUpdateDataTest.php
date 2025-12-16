<?php

/**
 * This file contains the MariaDBTargetUpdateDataTest class.
 *
 * SPDX-FileCopyrightText: Copyright 2025 Framna Netherlands B.V., Zwolle, The Netherlands
 * SPDX-License-Identifier: MIT
 */

namespace Pipeline\Import\MariaDB\Tests;

use Lunr\Gravity\Tests\Helpers\DatabaseAccessObjectQueryTestTrait;
use Pipeline\Import\ContentRangeInterface;
use Pipeline\Import\DataDiffCategory;

/**
 * This class contains the tests for the MariaDBTarget.
 *
 * @covers Pipeline\Import\MariaDB\MariaDBTarget
 */
class MariaDBTargetUpdateDataTest extends MariaDBTargetTestCase
{

    use DatabaseAccessObjectQueryTestTrait;

    private $item = [
        'id'   => '0647ab6d-5d08-685a-9b2f-ff000058806b',
        'name' => 'Item',
    ];

    /**
     * Test that updateData() throws an exception if the table name is undefined.
     *
     * @covers Pipeline\Import\MariaDB\MariaDBTarget::updateData
     */
    public function testUpdateDataThrowsExceptionIfTableUndefined(): void
    {
        $data = [
            DataDiffCategory::New->value      => [
                $this->item,
            ],
            DataDiffCategory::Updated->value  => [],
            DataDiffCategory::Obsolete->value => [],
        ];

        $this->db->shouldReceive('begin_transaction')
                 ->never();

        $this->db->shouldReceive('get_new_dml_query_builder_object')
                 ->never();

        $this->db->shouldReceive('query')
                 ->never();

        $this->expectException('UnexpectedValueException');
        $this->expectExceptionMessage('No table defined to import to!');

        $this->class->updateData($data);
    }

    /**
     * Test that updateData() throws an exception if inserting/updating data fails.
     *
     * @covers Pipeline\Import\MariaDB\MariaDBTarget::updateData
     */
    public function testUpdateDataThrowsExceptionIfReplacePartFailed(): void
    {
        $data = [
            DataDiffCategory::New->value      => [
                $this->item,
            ],
            DataDiffCategory::Updated->value  => [],
            DataDiffCategory::Obsolete->value => [],
        ];

        $this->setReflectionPropertyValue('table', 'table');

        $this->db->shouldReceive('begin_transaction')
                 ->once();

        $this->expectQueryError();

        $this->expectException('Lunr\Gravity\Exceptions\QueryException');
        $this->expectExceptionMessage('Database query error!');

        $this->class->updateData($data);
    }

    /**
     * Test that updateData() throws an exception if deleting data fails.
     *
     * @covers Pipeline\Import\MariaDB\MariaDBTarget::updateData
     */
    public function testUpdateDataThrowsExceptionIfDeletePartFailed(): void
    {
        $data = [
            DataDiffCategory::New->value      => [],
            DataDiffCategory::Updated->value  => [],
            DataDiffCategory::Obsolete->value => [
                $this->item,
            ],
        ];

        $this->setReflectionPropertyValue('identifierKeys', [ 'id' ]);
        $this->setReflectionPropertyValue('table', 'table');

        $this->expectException('Lunr\Gravity\Exceptions\QueryException');
        $this->expectExceptionMessage('Database query error!');

        $this->db->shouldReceive('begin_transaction')
                 ->once();

        $this->expectQueryError();

        $this->assertFalse($this->class->updateData($data));
    }

    /**
     * Test that updateData() returns number of affected rows on success.
     *
     * @covers Pipeline\Import\MariaDB\MariaDBTarget::updateData
     */
    public function testUpdateDataReturnsNumberOfAffectedRowsOnSuccess(): void
    {
        $data = [
            DataDiffCategory::New->value      => [
                $this->item,
            ],
            DataDiffCategory::Updated->value  => [],
            DataDiffCategory::Obsolete->value => [
                $this->item,
            ],
        ];

        $this->setReflectionPropertyValue('identifierKeys', [ 'id' ]);
        $this->setReflectionPropertyValue('table', 'table');

        $this->db->shouldReceive('begin_transaction')
                 ->once();

        $this->db->shouldReceive('rollback')
                 ->never();

        $this->db->shouldReceive('end_transaction')
                 ->once();

        $this->db->shouldReceive('get_new_dml_query_builder_object')
                 ->once()
                 ->andReturn($this->realSimpleBuilder);

        $this->db->shouldReceive('get_new_dml_query_builder_object')
                 ->once()
                 ->with(FALSE)
                 ->andReturn($this->realBuilder);

        $this->db->shouldReceive('query')
                 ->zeroOrMoreTimes()
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

        $this->result->shouldReceive('affected_rows')
                     ->once()
                     ->andReturn(20);

        $this->result->shouldReceive('affected_rows')
                     ->once()
                     ->andReturn(30);

        $this->assertSame(50, $this->class->updateData($data));
    }

    /**
     * Test that updateData() with ranges.
     *
     * @covers Pipeline\Import\MariaDB\MariaDBTarget::updateData
     */
    public function testUpdateDataWithRanges(): void
    {
        $data = [
            DataDiffCategory::New->value      => [
                $this->item,
            ],
            DataDiffCategory::Updated->value  => [],
            DataDiffCategory::Obsolete->value => [
                $this->item,
            ],
        ];

        $range1 = $this->getMockBuilder(ContentRangeInterface::class)
                       ->getMock();

        $range2 = $this->getMockBuilder(ContentRangeInterface::class)
                       ->getMock();

        $ranges = [ $range1, $range2 ];

        $this->setReflectionPropertyValue('identifierKeys', [ 'id' ]);
        $this->setReflectionPropertyValue('table', 'table');

        $this->db->shouldReceive('begin_transaction')
                 ->once();

        $this->db->shouldReceive('rollback')
                 ->never();

        $this->db->shouldReceive('end_transaction')
                 ->once();

        $this->db->shouldReceive('get_new_dml_query_builder_object')
                 ->once()
                 ->andReturn($this->realSimpleBuilder);

        $this->db->shouldReceive('get_new_dml_query_builder_object')
                 ->once()
                 ->with(FALSE)
                 ->andReturn($this->realBuilder);

        $this->db->shouldReceive('query')
                 ->zeroOrMoreTimes()
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

        $this->result->shouldReceive('affected_rows')
                     ->once()
                     ->andReturn(20);

        $this->result->shouldReceive('affected_rows')
                     ->once()
                     ->andReturn(30);

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

        $this->assertSame(50, $this->class->updateData($data, $ranges));
    }

}

?>
