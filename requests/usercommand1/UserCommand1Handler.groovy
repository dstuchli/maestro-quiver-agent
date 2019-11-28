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

    static void convertQuiverResults(Integer senOrRec) throws IOException {
        String fileName
        BufferedReader br
        String path = "/maestro/agent/logs/last/"
        String sender = "sender-transfers.csv.xz"
        String receiver = "receiver-transfers.csv.xz"

        if (senOrRec == 1) {
            br = new BufferedReader(new FileReader("${path}${sender}"))
            fileName = "sender.dat"
        } else {
            br = new BufferedReader(new FileReader("${path}${receiver}"))
            fileName = "receiver.dat"
        }

        BufferedWriter bw = new BufferedWriter(new FileWriter(fileName))
        PrintWriter pw = new PrintWriter(bw)
        String line
        String[] previousLine = ["0","-1"]
        long lastTimestampMessageCount = 0

        while ((line = br.readLine()) != null) {
            String[] currentLine = line.split(",")

            // first line of the csv file
            if (previousLine[1] == "-1") {
                previousLine = currentLine
                continue
            }

            if (currentLine[1].toLong() != previousLine[1].toLong()) {
                pw.printf("0,%l,%s", previousLine[0].toLong() - lastTimestampMessageCount, previousLine[1])
                lastTimestampMessageCount = previousLine[0].toLong()
            }

            previousLine = currentLine
        }

        // last line of the csv file
        pw.printf("0,%l,%s", previousLine[0].toLong() - lastTimestampMessageCount, previousLine[1])

        pw.close()
        br.close()

    }


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

            convertQuiverResults(1)
            convertQuiverResults(0)

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