/*
 *  Copyright 2017 Otavio R. Piske <angusyoung@gmail.com>
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this normalizedFile except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.maestro.agent.ext.requests.genericrequest

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.maestro.client.notes.*;

import org.maestro.agent.base.AbstractHandler


class UserCommand1Handler extends AbstractHandler {
    private static final Logger logger = LoggerFactory.getLogger(UserCommand1Handler.class);

    @Override
    Object handle() {
        String logDir = getTestLogDir().getPath()

        logger.info("Running Quiver")
        String command = "quiver --output ${logDir}"

        UserCommand1Request request = (UserCommand1Request) getNote()
        String arrow = request.getPayload()

        if (arrow != null) {
            logger.info("Using quiver arrow {}", arrow)
            command = command + " --arrow " + arrow
        }

        try {
            command = command + " " + getWorkerOptions().getBrokerURL()
            if (super.executeOnShell(command) != 0) {
                logger.warn("Unable to execute the Quiver test")
                this.getClient().notifyFailure(getCurrentTest(), "Unable to execute the Quiver test")

                return null
            }

            createTestSuccessSymlinks();

            String cloneConverterCommand = "curl -L https://github.com/dstuchli/quiver-results-converter/releases/download/1.0/quiver-results-converter-1.0-SNAPSHOT-bin.tar.gz --output quiver-results-converter-1.0-SNAPSHOT-bin.tar.gz"
            if (super.executeOnShell(cloneConverterCommand) == 0) {
                String untar = "tar -xvf quiver-results-converter-1.0-SNAPSHOT-bin.tar.gz"
                if (super.executeOnShell(untar) == 0) {

                    String runScriptSender = "cd quiver-results-converter-1.0-SNAPSHOT/bin/ & ./quiver-results-converter.sh convert /maestro/agent/logs/lastSuccessful/sender-transfers.csv.xz /maestro/agent/logs/lastSuccessful/sender-summary.json"
                    if (super.executeOnShell(runScriptSender) != 0) {
                        logger.warn("Unable to convert sender files")
                    }

                    String runScriptReceiver = "cd quiver-results-converter-1.0-SNAPSHOT/bin/ & ./quiver-results-converter.sh convert /maestro/agent/logs/lastSuccessful/receiver-transfers.csv.xz /maestro/agent/logs/lastSuccessful/last/receiver-summary.json"
                    if (super.executeOnShell(runScriptReceiver) != 0) {
                        logger.warn("Unable to convert receiver files")
                    }

                } else {
                    logger.warn("Unable to extract the converter files")
                }
            } else {
                logger.warn("Unable to download the converter")
            }


            this.getClient().notifySuccess(getCurrentTest(), "Quiver test ran successfully")
            logger.info("Quiver test ran successfully")

        }
        catch (Exception e) {
            createTestFailSymlinks();

            this.getClient().notifyFailure(getCurrentTest(), e.getMessage())

            return null
        }

        return null
    }
}