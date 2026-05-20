package com.dispatch.service;

import com.dispatch.web.dto.PickupLocation;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class HardcodedDriverSelectionStrategyTest {

    private final HardcodedDriverSelectionStrategy strategy =
        new HardcodedDriverSelectionStrategy();

    private static final PickupLocation DUMMY_PICKUP = new PickupLocation(51.5, -0.1);

    @Test
    void selectDriver_firstCall_returnsDriver001() {
        assertEquals("driver-001", strategy.selectDriver(DUMMY_PICKUP));
    }

    @Test
    void selectDriver_roundRobinCyclesThroughAllDrivers() {
        HardcodedDriverSelectionStrategy s = new HardcodedDriverSelectionStrategy();
        Set<String> seen = new HashSet<>();
        for (int i = 0; i < 3; i++) {
            seen.add(s.selectDriver(DUMMY_PICKUP));
        }
        assertEquals(3, seen.size(), "All 3 drivers should be selected in one cycle");
        assertTrue(seen.contains("driver-001"));
        assertTrue(seen.contains("driver-002"));
        assertTrue(seen.contains("driver-003"));
    }

    @Test
    void selectDriver_wrapsBackToDriver001AfterThreeCalls() {
        HardcodedDriverSelectionStrategy s = new HardcodedDriverSelectionStrategy();
        s.selectDriver(DUMMY_PICKUP); // driver-001
        s.selectDriver(DUMMY_PICKUP); // driver-002
        s.selectDriver(DUMMY_PICKUP); // driver-003
        assertEquals("driver-001", s.selectDriver(DUMMY_PICKUP), "Should wrap back to driver-001");
    }

    @Test
    void selectDriver_nullPickup_doesNotThrow() {
        // Phase 1: pickup is ignored, so null should not cause NPE.
        assertDoesNotThrow(() -> strategy.selectDriver(null));
    }

    @Test
    void selectDriver_returnsNonNullNonBlankDriver() {
        String driver = strategy.selectDriver(DUMMY_PICKUP);
        assertNotNull(driver);
        assertFalse(driver.isBlank());
    }
}
