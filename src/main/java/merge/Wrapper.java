package merge;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

class Wrapper {
    private BufferedReader br;
    private String line;

    Wrapper(File file) {
        try {
            br = new BufferedReader(new FileReader(file));
            line = br.readLine();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    void nextLine() {
        try {
            line = br.readLine();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    String getLine() {
        return line;
    }

    void close() {
        try {
            br.close();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
