<?php

/**
 * This file contains the ValueRangeApplyTest class.
 *
 * SPDX-FileCopyrightText: Copyright 2026 Framna Netherlands B.V., Zwolle, The Netherlands
 * SPDX-License-Identifier: MIT
 */

namespace Pipeline\Import\MariaDB\Ranges\Tests;

use Lunr\Gravity\MySQL\MySQLDMLQueryBuilder;
use Lunr\Gravity\MySQL\MySQLQueryEscaper;
use Mockery;
use Pipeline\Import\Exceptions\ContentRangeException;

/**
 * This class contains tests for the ValueRange class.
 *
 * @covers \Pipeline\Import\MariaDB\Ranges\ValueRange
 */
class ValueRangeApplyTest extends ValueRangeTestCase
{

    /**
     * Test that apply() throws an exception if there is no active query builder.
     *
     * @covers \Pipeline\Import\MariaDB\Ranges\ValueRange::isEmpty
     */
    public function testApplyThrowsExceptionIfBuilderIsNull(): void
    {
        $escaper = Mockery::mock(MySQLQueryEscaper::class);

        $this->target->expects($this->once())
                     ->method('getQueryBuilder')
                     ->willReturn(NULL);

        $this->target->expects($this->once())
                     ->method('getQueryEscaper')
                     ->willReturn($escaper);

        $this->expectException(ContentRangeException::class);
        $this->expectExceptionMessage('Tried to apply value range, but no query builder is active!');

        $this->class->apply();
    }

    /**
     * Test that apply() adds a data range to a query.
     *
     * @covers \Pipeline\Import\MariaDB\Ranges\ValueRange::isEmpty
     */
    public function testApplyAddsDataRangeToQuery(): void
    {
        $values = [ 'value_1', 'value_2' ];
        $key    = 'bar';

        $escaped = '("value_1", "value_2")';

        $this->setReflectionPropertyValue('key', $key);
        $this->setReflectionPropertyValue('values', $values);

        $builder = Mockery::mock(MySQLDMLQueryBuilder::class);
        $escaper = Mockery::mock(MySQLQueryEscaper::class);

        $this->target->expects($this->once())
                     ->method('getQueryBuilder')
                     ->willReturn($builder);

        $this->target->expects($this->once())
                     ->method('getQueryEscaper')
                     ->willReturn($escaper);

        $escaper->shouldReceive('value')
                ->once()
                ->with('value_1')
                ->andReturnArg(0);

        $escaper->shouldReceive('value')
                ->once()
                ->with('value_2')
                ->andReturnArg(0);

        $escaper->shouldReceive('column')
                ->once()
                ->with('bar')
                ->andReturn('`bar`');

        $escaper->shouldReceive('list_value')
                ->once()
                ->with($values)
                ->andReturn($escaped);

        $builder->shouldReceive('where_in')
                ->once()
                ->with('`bar`', $escaped);

        $this->class->apply($builder, $escaper);
    }

}

?>
