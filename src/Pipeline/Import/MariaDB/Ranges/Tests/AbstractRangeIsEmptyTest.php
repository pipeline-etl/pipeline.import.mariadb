<?php

/**
 * This file contains the AbstractRangeIsEmptyTest class.
 *
 * SPDX-FileCopyrightText: Copyright 2026 Framna Netherlands B.V., Zwolle, The Netherlands
 * SPDX-License-Identifier: MIT
 */

namespace Pipeline\Import\MariaDB\Ranges\Tests;

/**
 * This class contains tests for the AbstractRange class.
 *
 * @covers Pipeline\Import\MariaDB\Ranges\AbstractRange
 */
class AbstractRangeIsEmptyTest extends AbstractRangeTestCase
{

    /**
     * Test that isEmpty() returns TRUE for empty range.
     *
     * @covers \Pipeline\Import\MariaDB\Ranges\AbstractRange::isEmpty
     */
    public function testIsEmptyReturnsTrueForEmptyRange(): void
    {
        $this->assertTrue($this->class->isEmpty());
    }

    /**
     * Test that isEmpty() returns FALSE for non-empty range.
     *
     * @covers \Pipeline\Import\MariaDB\Ranges\AbstractRange::isEmpty
     */
    public function testIsEmptyReturnsFalseForNonEmptyRange(): void
    {
        $this->setReflectionPropertyValue('data', [ [ 'item_1' ], [ 'item_2' ] ]);

        $this->assertFalse($this->class->isEmpty());
    }

}

?>
