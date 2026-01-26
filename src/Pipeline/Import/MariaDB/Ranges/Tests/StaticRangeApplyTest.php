<?php

/**
 * This file contains the StaticRangeApplyTest class.
 *
 * SPDX-FileCopyrightText: Copyright 2026 Framna Netherlands B.V., Zwolle, The Netherlands
 * SPDX-License-Identifier: MIT
 */

namespace Pipeline\Import\MariaDB\Ranges\Tests;

use Lunr\Gravity\MySQL\MySQLDMLQueryBuilder;
use Lunr\Gravity\MySQL\MySQLQueryEscaper;
use Mockery;
use Pipeline\Common\Node;
use Pipeline\Import\Exceptions\ContentRangeException;

/**
 * This class contains tests for the StaticRange class.
 *
 * @phpstan-import-type ProcessedItem from Node
 *
 * @covers Pipeline\Import\MariaDB\Ranges\StaticRange
 */
class StaticRangeApplyTest extends StaticRangeTestCase
{

    /**
     * Test that apply() throws an exception if there is no active query builder.
     *
     * @covers \Pipeline\Import\MariaDB\Ranges\StaticRange::apply
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
        $this->expectExceptionMessage('Tried to apply static range, but no query builder is active!');

        $this->class->apply();
    }

    /**
     * Test that apply() adds a data range to a query.
     *
     * @covers \Pipeline\Import\MariaDB\Ranges\StaticRange::apply
     */
    public function testApplyAddsDataRangeToQuery(): void
    {
        $field = 'bar';
        $value = 'foo';

        $this->setReflectionPropertyValue('field', $field);
        $this->setReflectionPropertyValue('value', $value);

        $builder = Mockery::mock(MySQLDMLQueryBuilder::class);
        $escaper = Mockery::mock(MySQLQueryEscaper::class);

        $this->target->expects($this->once())
                     ->method('getQueryBuilder')
                     ->willReturn($builder);

        $this->target->expects($this->once())
                     ->method('getQueryEscaper')
                     ->willReturn($escaper);

        $escaper->shouldReceive('column')
                ->once()
                ->with('bar')
                ->andReturn('`bar`');

        $escaper->shouldReceive('value')
                ->once()
                ->with('foo')
                ->andReturn("'foo'");

        $builder->shouldReceive('where')
                ->once()
                ->with('`bar`', "'foo'");

        $this->class->apply($builder, $escaper);
    }

}

?>
