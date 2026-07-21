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
package com.highkeen.opendcb.bootstrap.axon.postgres;

import org.axonframework.eventsourcing.annotation.EventSourcedEntity;
import org.axonframework.eventsourcing.annotation.reflection.EntityCreator;
import org.axonframework.eventsourcing.configuration.EventSourcedEntityModule;
import org.axonframework.eventsourcing.configuration.EventSourcingConfigurer;
import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;

/**
 * Minimal event-sourced entity used to prove {@link OpenDcbAxonPostgres#engine} works end-to-end against a real
 * PostgreSQL instance: one command, one event, one field worth asserting on after re-sourcing.
 */
@EventSourcedEntity
public class AccountEntity {

    public static EventSourcingConfigurer configurer() {
        return EventSourcingConfigurer.create()
                .registerEntity(EventSourcedEntityModule.autodetected(String.class, AccountEntity.class));
    }

    @CommandHandler
    static void handle(OpenAccount cmd, EventAppender eventAppender) {
        eventAppender.append(new AccountOpened(cmd.accountId(), cmd.ownerName()));
    }

    private final String accountId;
    private final String ownerName;

    @EntityCreator
    public AccountEntity(AccountOpened event) {
        this.accountId = event.accountId();
        this.ownerName = event.ownerName();
    }

    public String accountId() {
        return accountId;
    }

    public String ownerName() {
        return ownerName;
    }
}
