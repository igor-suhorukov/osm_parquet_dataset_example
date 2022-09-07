package com.github.igorsuhorukov.arrow.osm.example;

import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionMapReader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.util.Text;

import java.io.File;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.StreamSupport;

public class CalculateSchoolCount {

    public static final int BATCH_SIZE = 100000;
    public static final Text BUILDING_KEY = new Text("building");
    public static final Text SCHOOL_VALUE = new Text("school");
    public static final Text AMENITY_KEY = new Text("amenity");

    public static void main(String[] args) throws Exception{
        if(args.length!=1){
            throw new IllegalArgumentException("Specify source dataset path for parquet files");
        }
        File datasetPath = new File(args[0]);
        if(!datasetPath.exists()){
            throw new IllegalArgumentException();
        }

        long startTime = System.currentTimeMillis();
        try (BufferAllocator allocator = new RootAllocator()) {
            FileSystemDatasetFactory factory = new FileSystemDatasetFactory(allocator,
                    NativeMemoryPool.getDefault(), FileFormat.PARQUET, datasetPath.toURI().toURL().toExternalForm());
            final Dataset dataset = factory.finish();
            ScanOptions options = new ScanOptions(BATCH_SIZE);
            final Scanner scanner = dataset.newScan(options);
            try {
                AtomicLong totalSchoolCnt = new AtomicLong();
                StreamSupport.stream(scanner.scan().spliterator(), true).forEach(scanTask -> {
                    long schoolCnt=0;
                    try (ArrowReader reader = scanTask.execute()) {
                        while (reader.loadNextBatch()) {
                            VectorSchemaRoot root = reader.getVectorSchemaRoot();
                            BitVector closed = (BitVector) root.getVector("closed");
                            MapVector tags = (MapVector) root.getVector("tags");
                            UnionMapReader tagsReader = tags.getReader();
                            for(int row=0, size = root.getRowCount(); row < size; row++){
                                if(closed.get(row) != 0){
                                    tagsReader.setPosition(row);
                                    boolean building=false;
                                    boolean buildingSchool=false;
                                    boolean amenitySchool=false;
                                    while (tagsReader.next()){
                                        Text key = (Text) tagsReader.key().readObject();
                                        Text value = (Text) tagsReader.value().readObject();
                                        if(key!=null && key.equals(BUILDING_KEY)){
                                            if(value!=null && value.equals(SCHOOL_VALUE)){
                                                buildingSchool = true;
                                                break;
                                            }
                                            building=true;
                                            if(amenitySchool){
                                                break;
                                            }
                                        }
                                        if(key!=null && value!=null && key.equals(AMENITY_KEY) && value.equals(SCHOOL_VALUE)){
                                            amenitySchool = true;
                                            if(building){
                                                break;
                                            }
                                        }
                                    }
                                    if(buildingSchool || (building && amenitySchool)){
                                        schoolCnt++;
                                    }
                                }
                            }
                            tags.close();
                            closed.close();
                            root.close();
                        }
                        totalSchoolCnt.addAndGet(schoolCnt);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        AutoCloseables.closeNoChecked(scanTask);
                    }
                });
                long executionTime = System.currentTimeMillis() - startTime;
                System.out.println(totalSchoolCnt.get()+" ("+executionTime+" ms)");

            } finally {
                AutoCloseables.close(scanner, dataset);
            }
        }
    }
}
