<?php

/**
 * This file contains the IdentifierRangeApplyTest class.
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
 * This class contains tests for the IdentifierRange class.
 *
 * @phpstan-import-type ProcessedItem from Node
 *
 * @covers Pipeline\Import\MariaDB\Ranges\IdentifierRange
 */
class IdentifierRangeApplyTest extends IdentifierRangeTestCase
{

    /**
     * Expected identifier for the test data.
     * @var ProcessedItem
     */
    private array $identifier = [
        'id'        => 1,
        'category'  => 'Bar',
        'enabled'   => TRUE,
    ];

    /**
     * Test that apply() throws an exception if there is no active query builder.
     *
     * @covers \Pipeline\Import\MariaDB\Ranges\IdentifierRange::apply
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
        $this->expectExceptionMessage('Tried to apply identifier range, but no query builder is active!');

        $this->class->apply();
    }

    /**
     * Test that apply() adds a data range to a query.
     *
     * @covers \Pipeline\Import\MariaDB\Ranges\IdentifierRange::apply
     */
    public function testApplyAddsDataRangeToQueryForSingleItem(): void
    {
        $builder = Mockery::mock(MySQLDMLQueryBuilder::class);
        $escaper = Mockery::mock(MySQLQueryEscaper::class);

        $data = [ $this->identifier ];

        $this->setReflectionPropertyValue('data', $data);

        $this->target->expects($this->once())
                     ->method('getQueryBuilder')
                     ->willReturn($builder);

        $this->target->expects($this->once())
                     ->method('getQueryEscaper')
                     ->willReturn($escaper);

        $builder->shouldReceive('start_where_group')
                ->once();

        $escaper->shouldReceive('column')
                ->once()
                ->with('id')
                ->andReturn('`id`');

        $escaper->shouldReceive('column')
                ->once()
                ->with('category')
                ->andReturn('`category`');

        $escaper->shouldReceive('column')
                ->once()
                ->with('enabled')
                ->andReturn('`enabled`');

        $escaper->shouldReceive('value')
                ->once()
                ->with(1)
                ->andReturn("'1'");

        $escaper->shouldReceive('value')
                ->once()
                ->with('Bar')
                ->andReturn("'Bar'");

        $escaper->shouldReceive('intvalue')
                ->once()
                ->with(TRUE)
                ->andReturn('1');

        $builder->shouldReceive('where')
                ->once()
                ->with('`id`', "'1'");

        $builder->shouldReceive('where')
                ->once()
                ->with('`category`', "'Bar'");

        $builder->shouldReceive('where')
                ->once()
                ->with('`enabled`', '1');

        $builder->shouldReceive('end_where_group')
                ->once();

        $builder->shouldReceive('or')
                ->once();

        $this->class->apply();
    }

    /**
     * Test that apply() adds a data range to a query.
     *
     * @covers \Pipeline\Import\MariaDB\Ranges\IdentifierRange::apply
     */
    public function testApplyAddsDataRangeToQueryForMultipleItems(): void
    {
        $builder = Mockery::mock(MySQLDMLQueryBuilder::class);
        $escaper = Mockery::mock(MySQLQueryEscaper::class);

        $identifier = $this->identifier;

        $identifier['id']      = 2;
        $identifier['enabled'] = FALSE;

        $data = [ $this->identifier, $identifier ];

        $this->setReflectionPropertyValue('data', $data);

        $this->target->expects($this->once())
                     ->method('getQueryBuilder')
                     ->willReturn($builder);

        $this->target->expects($this->once())
                     ->method('getQueryEscaper')
                     ->willReturn($escaper);

        $builder->shouldReceive('start_where_group')
                ->twice();

        $escaper->shouldReceive('column')
                ->twice()
                ->with('id')
                ->andReturn('`id`');

        $escaper->shouldReceive('column')
                ->twice()
                ->with('category')
                ->andReturn('`category`');

        $escaper->shouldReceive('column')
                ->twice()
                ->with('enabled')
                ->andReturn('`enabled`');

        $escaper->shouldReceive('value')
                ->once()
                ->with(1)
                ->andReturn("'1'");

        $escaper->shouldReceive('value')
                ->twice()
                ->with('Bar')
                ->andReturn("'Bar'");

        $escaper->shouldReceive('intvalue')
                ->once()
                ->with(TRUE)
                ->andReturn('1');

        $builder->shouldReceive('where')
                ->once()
                ->with('`id`', "'1'");

        $builder->shouldReceive('where')
                ->twice()
                ->with('`category`', "'Bar'");

        $builder->shouldReceive('where')
                ->once()
                ->with('`enabled`', '1');

        $builder->shouldReceive('end_where_group')
                ->twice();

        $builder->shouldReceive('or')
                ->twice();

        $escaper->shouldReceive('value')
                ->once()
                ->with(2)
                ->andReturn("'2'");

        $escaper->shouldReceive('intvalue')
                ->once()
                ->with(FALSE)
                ->andReturn('0');

        $builder->shouldReceive('where')
                ->once()
                ->with('`id`', "'2'");

        $builder->shouldReceive('where')
                ->once()
                ->with('`enabled`', '0');

        $this->class->apply();
    }

}

?>
