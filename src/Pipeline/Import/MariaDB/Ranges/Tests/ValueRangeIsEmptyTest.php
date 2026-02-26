<?php

/**
 * This file contains the ValueRangeIsEmptyTest class.
 *
 * SPDX-FileCopyrightText: Copyright 2026 Framna Netherlands B.V., Zwolle, The Netherlands
 * SPDX-License-Identifier: MIT
 */

namespace Pipeline\Import\MariaDB\Ranges\Tests;

/**
 * This class contains tests for the ValueRange class.
 *
 * @covers Pipeline\Import\MariaDB\Ranges\ValueRange
 */
class ValueRangeIsEmptyTest extends ValueRangeTestCase
{

    /**
     * Test that isEmpty() returns TRUE for empty range.
     *
     * @covers \Pipeline\Import\MariaDB\Ranges\ValueRange::isEmpty
     */
    public function testIsEmptyReturnsTrueForEmptyRange(): void
    {
        $this->assertTrue($this->class->isEmpty());
    }

    /**
     * Test that isEmpty() returns FALSE for non-empty range.
     *
     * @covers \Pipeline\Import\MariaDB\Ranges\ValueRange::isEmpty
     */
    public function testIsEmptyReturnsFalseForNonEmptyRange(): void
    {
        $this->setReflectionPropertyValue('values', [ 'value_1', 'value_2' ]);

        $this->assertFalse($this->class->isEmpty());
    }

}

?>
