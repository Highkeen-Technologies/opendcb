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
package com.highkeen.opendcb.examples.plainjava;

import org.axonframework.messaging.commandhandling.annotation.CommandHandler;
import org.axonframework.messaging.eventhandling.gateway.EventAppender;
import org.axonframework.modelling.annotation.InjectEntity;

/**
 * Stateful Command Handler: a class separate from {@link AccountEntity}, registered independently via
 * {@code CommandHandlingModule} rather than as command handlers declared on the entity itself. The
 * creational handler needs no injected entity — nothing exists yet for {@code OpenAccount} to load. The
 * instance handler injects the current {@link AccountEntity} via {@link InjectEntity} to make its decision.
 */
class AccountCommandHandlers {

    @CommandHandler
    void handle(OpenAccount command, EventAppender eventAppender) {
        eventAppender.append(new AccountOpened(command.accountId(), command.ownerName()));
    }

    @CommandHandler
    void handle(DepositMoney command, @InjectEntity AccountEntity account, EventAppender eventAppender) {
        if (command.amount().signum() <= 0) {
            throw new IllegalArgumentException("Deposit amount must be positive, got " + command.amount());
        }
        System.out.println("  [handler] depositing into " + account.ownerName()
                + "'s account (balance before this deposit: " + account.balance() + ")");
        eventAppender.append(new MoneyDeposited(command.accountId(), command.amount()));
    }
}
