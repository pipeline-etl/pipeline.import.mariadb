<?php

/**
 * This file contains the StaticRangeIsEmptyTest class.
 *
 * SPDX-FileCopyrightText: Copyright 2026 Framna Netherlands B.V., Zwolle, The Netherlands
 * SPDX-License-Identifier: MIT
 */

namespace Pipeline\Import\MariaDB\Ranges\Tests;

/**
 * This class contains tests for the StaticRange class.
 *
 * @covers Pipeline\Import\MariaDB\Ranges\StaticRange
 */
class StaticRangeIsEmptyTest extends StaticRangeTestCase
{

    /**
     * Test that isEmpty() returns FALSE.
     *
     * @covers \Pipeline\Import\MariaDB\Ranges\StaticRange::isEmpty
     */
    public function testIsEmptyReturnsFalse(): void
    {
        $this->assertFalse($this->class->isEmpty());
    }

}

?>
