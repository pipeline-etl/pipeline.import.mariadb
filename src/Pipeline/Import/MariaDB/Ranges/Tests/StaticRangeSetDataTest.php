<?php

/**
 * This file contains the StaticRangeSetDataTest class.
 *
 * SPDX-FileCopyrightText: Copyright 2026 Framna Netherlands B.V., Zwolle, The Netherlands
 * SPDX-License-Identifier: MIT
 */

namespace Pipeline\Import\MariaDB\Ranges\Tests;

use Pipeline\Common\Node;
use Pipeline\Import\Exceptions\ContentRangeException;

/**
 * This class contains tests for the StaticRange class.
 *
 * @phpstan-import-type ProcessedItem from Node
 *
 * @covers Pipeline\Import\MariaDB\Ranges\StaticRange
 */
class StaticRangeSetDataTest extends StaticRangeTestCase
{

    /**
     * Test that setData() throws an exception if field is missing.
     *
     * @covers \Pipeline\Import\MariaDB\Ranges\StaticRange::setData
     */
    public function testSetDataWithMissingField(): void
    {
        $this->expectException(ContentRangeException::class);
        $this->expectExceptionMessage('Static range requires a field value!');

        $input = [];

        $this->class->setData($input, [ 'value' => 'foo' ]);
    }

    /**
     * Test that setData() throws an exception if field is empty.
     *
     * @covers \Pipeline\Import\MariaDB\Ranges\StaticRange::setData
     */
    public function testSetDataWithEmptyField(): void
    {
        $this->expectException(ContentRangeException::class);
        $this->expectExceptionMessage('Static range requires a field value!');

        $input = [];

        $this->class->setData($input, [ 'field' => NULL, 'value' => 'foo' ]);
    }

    /**
     * Test that setData() throws an exception if value is missing.
     *
     * @covers \Pipeline\Import\MariaDB\Ranges\StaticRange::setData
     */
    public function testSetDataWithMissingValue(): void
    {
        $this->expectException(ContentRangeException::class);
        $this->expectExceptionMessage('Static range requires a column value!');

        $input = [];

        $this->class->setData($input, [ 'field' => 'bar' ]);
    }

    /**
     * Test that setData() throws an exception if value is empty.
     *
     * @covers \Pipeline\Import\MariaDB\Ranges\StaticRange::setData
     */
    public function testSetDataWithEmptyValue(): void
    {
        $this->expectException(ContentRangeException::class);
        $this->expectExceptionMessage('Static range requires a column value!');

        $input = [];

        $this->class->setData($input, [ 'field' => 'bar', 'value' => NULL ]);
    }

    /**
     * Test that setData() sets filtered data.
     *
     * @covers \Pipeline\Import\MariaDB\Ranges\StaticRange::setData
     */
    public function testSetDataFiltersItemByStaticValue(): void
    {
        $input = [];

        $this->class->setData($input, [ 'field' => 'bar', 'value' => 'foo' ]);

        $field = $this->getReflectionPropertyValue('field');
        $value = $this->getReflectionPropertyValue('value');

        $this->assertEquals('bar', $field);
        $this->assertEquals('foo', $value);
    }

    /**
     * Test that setData() sets filtered data.
     *
     * @covers \Pipeline\Import\MariaDB\Ranges\StaticRange::setData
     */
    public function testSetDataFiltersItemByBooleanStaticValue(): void
    {
        $input = [];

        $this->class->setData($input, [ 'field' => 'active', 'value' => FALSE ]);

        $field = $this->getReflectionPropertyValue('field');
        $value = $this->getReflectionPropertyValue('value');

        $this->assertEquals('active', $field);
        $this->assertSame(0, $value);
    }

}

?>
