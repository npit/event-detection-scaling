/*
 * Copyright 2015 SciFY NPO <info@scify.org>.
 *
 * This product is part of the NewSum Free Software.
 * For more information about NewSum visit
 *
 * 	http://www.scify.gr/site/en/projects/completed/newsum
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * If this code or its output is used, extended, re-engineered, integrated,
 * or embedded to any extent in another software or hardware, there MUST be
 * an explicit attribution to this work in the resulting source code,
 * the packaging (where such packaging exists), or user interface
 * (where such an interface exists).
 *
 * The attribution must be of the form "Powered by NewSum, SciFY"
 *
 */
package gr.demokritos.iit.crawlers.twitter.structures;

import java.util.Objects;
import net.arnx.jsonic.JSON;

/**
 *
 * @author George K. <gkiom@scify.org>
 */
public class SourceAccount {

    public static final String SOURCE = "account_name";
    public static final String ACTIVE = "active";

    private String account;
    private Boolean active;

    /**
     *
     * @param account the screen_name
     * @param active if true, will be used for monitor
     */
    public SourceAccount(String account, Boolean active) {
        this.account = account;
        this.active = active;
    }

    public String getAccount() {
        return account;
    }

    public Boolean getActive() {
        return active;
    }

    public void setActive(Boolean active) {
        this.active = active;
    }

    @Override
    public String toString() {
        return "SourceAccount{" + "account=" + account + ", active=" + active + "}";
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 71 * hash + Objects.hashCode(this.account);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final SourceAccount other = (SourceAccount) obj;
        if (!Objects.equals(this.account, other.account)) {
            return false;
        }
        return true;
    }

    public String toJSON() {
        return JSON.encode(this, false);
    }
}
