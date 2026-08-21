/*
 * Copyright the OpenDCB contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.highkeen.opendcb.snapshot.postgres.endtoend;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.axonframework.eventsourcing.annotation.EventSourcedEntity;
import org.axonframework.eventsourcing.annotation.EventSourcingHandler;
import org.axonframework.eventsourcing.annotation.Snapshotting;
import org.axonframework.eventsourcing.annotation.reflection.EntityCreator;

/**
 * Holds state only -- command handling lives in {@link CounterCommandHandlers}, same split as
 * {@code examples/plain-java-sample}'s {@code AccountEntity}/{@code AccountCommandHandlers}.
 *
 * <p>{@code @Snapshotting(afterEvents = 2)} triggers a snapshot store once a single sourcing/load
 * operation has applied more than 2 events -- per {@code SnapshotPolicy.afterEvents}'s real
 * semantics (confirmed from source), this is evaluated per-load, not as a cumulative count across
 * the whole log, so the end-to-end test dispatches enough {@link IncrementCounter} commands --
 * each one forcing a real load via {@code @InjectEntity} -- to cross that threshold within a single
 * load.
 *
 * <p>Carries a second, {@code @JsonCreator}-annotated constructor (distinct from the
 * {@code @EntityCreator} one Axon uses to build the entity from its creation event) purely so
 * {@link com.highkeen.opendcb.snapshot.postgres.PostgresSnapshotStore} can Jackson-(de)serialize
 * this class directly as a snapshot payload -- {@code SnapshottingEntityLifecycleHandler} stores
 * the live entity instance itself as the snapshot payload, not a separate DTO.
 */
@EventSourcedEntity
@Snapshotting(afterEvents = 2)
public class CounterEntity {

    private final String counterId;
    private int value;

    @EntityCreator
    public CounterEntity(CounterCreated event) {
        this.counterId = event.counterId();
        this.value = 0;
    }

    @JsonCreator
    public CounterEntity(@JsonProperty("counterId") String counterId, @JsonProperty("value") int value) {
        this.counterId = counterId;
        this.value = value;
    }

    @EventSourcingHandler
    void evolve(CounterIncremented event) {
        this.value++;
    }

    @JsonProperty("counterId")
    public String counterId() {
        return counterId;
    }

    @JsonProperty("value")
    public int value() {
        return value;
    }
}
