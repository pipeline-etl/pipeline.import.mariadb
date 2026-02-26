<?php

/**
 * This file contains the ValueRangeSetDataTest class.
 *
 * SPDX-FileCopyrightText: Copyright 2026 Framna Netherlands B.V., Zwolle, The Netherlands
 * SPDX-License-Identifier: MIT
 */

namespace Pipeline\Import\MariaDB\Ranges\Tests;

use Pipeline\Common\Node;
use Pipeline\Import\Exceptions\ContentRangeException;

/**
 * This class contains tests for the ValueRange class.
 *
 * @phpstan-import-type ProcessedItem from Node
 *
 * @covers Pipeline\Import\MariaDB\Ranges\ValueRange
 */
class ValueRangeSetDataTest extends ValueRangeTestCase
{

    /**
     * Test that setData() throws an exception if field is missing.
     *
     * @covers \Pipeline\Import\MariaDB\Ranges\ValueRange::setData
     */
    public function testSetDataWithMissingField(): void
    {
        $this->expectException(ContentRangeException::class);
        $this->expectExceptionMessage('Value range requires a field value!');

        $input = [];

        $this->class->setData($input, []);
    }

    /**
     * Test that setData() throws an exception if field is empty.
     *
     * @covers \Pipeline\Import\MariaDB\Ranges\ValueRange::setData
     */
    public function testSetDataWithEmptyField(): void
    {
        $this->expectException(ContentRangeException::class);
        $this->expectExceptionMessage('Value range requires a field value!');

        $input = [];

        $this->class->setData($input, [ 'field' => NULL ]);
    }

    /**
     * Test that setData() throws an exception if there are no values in the range.
     *
     * @covers \Pipeline\Import\MariaDB\Ranges\ValueRange::setData
     */
    public function testSetDataWithEmptyData(): void
    {
        $this->expectException(ContentRangeException::class);
        $this->expectExceptionMessage("Value range can't be empty!");

        $input = [];

        $this->class->setData($input, [ 'field' => 'bar' ]);
    }

    /**
     * Test that setData() throws an exception if there are only NULL values in the range.
     *
     * @covers \Pipeline\Import\MariaDB\Ranges\ValueRange::setData
     */
    public function testSetDataWithNullData(): void
    {
        $this->expectException(ContentRangeException::class);
        $this->expectExceptionMessage('Only NULL values in value range!');

        $input = [
            [
                'bar' => NULL,
            ],
        ];

        $this->class->setData($input, [ 'field' => 'bar' ]);
    }

    /**
     * Test that setData() properly prepares boolean values.
     *
     * @covers \Pipeline\Import\MariaDB\Ranges\ValueRange::setData
     */
    public function testSetDataWithBooleanData(): void
    {
        $input = [
            [
                'bar' => TRUE,
            ],
        ];

        $expected = [
            '1',
        ];

        $this->class->setData($input, [ 'field' => 'bar' ]);

        $this->assertPropertyEquals('values', $expected);
    }

    /**
     * Test that setData() deduplicates the values in the range.
     *
     * @covers \Pipeline\Import\MariaDB\Ranges\ValueRange::setData
     */
    public function testSetDataDeduplicatesData(): void
    {
        $input = [
            [
                'bar' => TRUE,
            ],
            [
                'bar' => 'string',
            ],
            [
                'bar' => TRUE,
            ],
            [
                'bar' => 'string',
            ],
            [
                'bar' => 1,
            ],
        ];

        $expected = [
            1,
            'string',
        ];

        $this->class->setData($input, [ 'field' => 'bar' ]);

        $this->assertPropertyEquals('values', $expected);
    }

    /**
     * Test that setData() doesn't include NULL in the data range.
     *
     * @covers \Pipeline\Import\MariaDB\Ranges\ValueRange::setData
     */
    public function testSetDataFiltersOutNullData(): void
    {
        $input = [
            [
                'bar' => TRUE,
            ],
            [
                'bar' => 'string',
            ],
            [
                'bar' => NULL,
            ],
            [
                'bar' => 'string',
            ],
            [
                'bar' => 1,
            ],
        ];

        $expected = [
            1,
            'string',
        ];

        $this->class->setData($input, [ 'field' => 'bar' ]);

        $this->assertPropertyEquals('values', $expected);
    }

}

?>
