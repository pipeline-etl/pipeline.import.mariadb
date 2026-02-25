<?php

/**
 * This file contains the IdentifierRangeSetDataTest class.
 *
 * SPDX-FileCopyrightText: Copyright 2026 Framna Netherlands B.V., Zwolle, The Netherlands
 * SPDX-License-Identifier: MIT
 */

namespace Pipeline\Import\MariaDB\Ranges\Tests;

use Pipeline\Common\Node;
use Pipeline\Import\Exceptions\ContentRangeException;

/**
 * This class contains tests for the IdentifierRange class.
 *
 * @phpstan-import-type ProcessedItem from Node
 *
 * @covers Pipeline\Import\MariaDB\Ranges\IdentifierRange
 */
class IdentifierRangeSetDataTest extends IdentifierRangeTestCase
{

    /**
     * Test data.
     * @var ProcessedItem
     */
    private array $data = [
        'id'        => 1,
        'category'  => 'Bar',
        'value'     => 'Foo',
        'timestamp' => 1768836579,
    ];

    /**
     * Expected identifier for the test data.
     * @var ProcessedItem
     */
    private array $identifier = [
        'id'        => 1,
        'category'  => 'Bar',
    ];

    /**
     * Test that setData() throws an exception if data is empty.
     *
     * @covers \Pipeline\Import\MariaDB\Ranges\IdentifierRange::setData
     */
    public function testSetDataWithEmptyData(): void
    {
        $input = [];

        $this->target->expects($this->once())
                     ->method('getIdentifierKeys')
                     ->willReturn(array_keys($this->identifier));

        $this->expectException(ContentRangeException::class);
        $this->expectExceptionMessage("Identifier range can't be empty!");

        $this->class->setData($input, []);
    }

    /**
     * Test that setData() sets filtered data.
     *
     * @covers \Pipeline\Import\MariaDB\Ranges\IdentifierRange::setData
     */
    public function testSetDataFiltersSingleItemByIdentifier(): void
    {
        $input = [ $this->data ];

        $this->target->expects($this->once())
                     ->method('getIdentifierKeys')
                     ->willReturn(array_keys($this->identifier));

        $this->class->setData($input, []);

        $result = $this->getReflectionPropertyValue('data');

        $this->assertEquals([ $this->identifier ], $result);
    }

    /**
     * Test that setData() sets filtered data.
     *
     * @covers \Pipeline\Import\MariaDB\Ranges\IdentifierRange::setData
     */
    public function testSetDataFiltersMultipleItemsByIdentifier(): void
    {
        $data       = $this->data;
        $identifier = $this->identifier;

        $data['id']       = 2;
        $identifier['id'] = 2;

        $input = [ $this->data, $data ];

        $this->target->expects($this->once())
                     ->method('getIdentifierKeys')
                     ->willReturn(array_keys($this->identifier));

        $this->class->setData($input, []);

        $result = $this->getReflectionPropertyValue('data');

        $this->assertEquals([ $this->identifier, $identifier ], $result);
    }

}

?>
