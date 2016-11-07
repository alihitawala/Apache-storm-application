package storm.starter;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by aliHitawala on 10/28/16.
 */
public class EnglishStopWords {
    private static EnglishStopWords ourInstance = new EnglishStopWords();
    private Set<String> stopWords = new HashSet<>();
    public static EnglishStopWords getInstance() {
        return ourInstance;
    }

    private EnglishStopWords() {
        String[] array = {"a","about","above","after","again","against","all","am","an","and","any","are","arent","as","at","be","because","been","before","being","below","between","both","but","by","cant","cannot","could","couldnt","did","didnt","do","does","doesnt","doing","dont","down","during","each","few","for","from","further","had","hadnt","has","hasnt","have","havent","having","he","hed","hell","hes","her","here","heres","hers","herself","him","himself","his","how","hows","i","id","ill","im","ive","if","in","into","is","isnt","it","its","its","itself","lets","me","more","most","mustnt","my","myself","no","nor","not","of","off","on","once","only","or","other","ought","our","ours"};
        this.stopWords.addAll(new ArrayList<String>(Arrays.asList(array)));
//        File testFile = new File("stopwords.txt");
//        if (!testFile.exists()) {
//            System.out.println("File stopwords.txt does not exist");
//            System.exit(0);
//        }
//        InputStream stream = EnglishStopWords.class.getResourceAsStream("stopwords.txt");
//        try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(testFile)))) {
//            String line;
//            while ((line = br.readLine()) != null) {
//                stopWords.add(line);
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    public Set<String> getStopWords() {
        return stopWords;
    }

    public static void main(String[] args) {
        System.out.println(EnglishStopWords.getInstance().getStopWords());
    }
}
