/*
 * Copyright 2013 gregory.graham.
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
 */
package nz.co.gregs.dbvolution.h2;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.SQLException;
import java.util.List;
import nz.co.gregs.dbvolution.example.Marque;
import org.junit.Test;

/**
 *
 * @author gregory.graham
 */
public class SerializableTest extends AbstractTest {

    String filename = "SerializableTest.obj";

    @Test
    public void saveToFile() throws SQLException {
        try {


            Marque hummerExample = new Marque();
            hummerExample.getUidMarque().blankQuery();
            hummerExample.getName().permittedValues("PEUGEOT", "HUMMER");
            List<Marque> marqueList = myDatabase.getDBTable(hummerExample).getRowsByExample(hummerExample).toList();

            ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filename));
            for (Object object : marqueList) {
                oos.writeObject(object);
            }
            oos.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        try {
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filename));
            while (ois.available() > 0) {
                Object object = ois.readObject();
                if (object instanceof Marque){
                    System.out.println(""+((Marque)object));
                }else{
                    throw new RuntimeException("Unable to reload the object correctly");
                }
            }
            ois.close();
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
