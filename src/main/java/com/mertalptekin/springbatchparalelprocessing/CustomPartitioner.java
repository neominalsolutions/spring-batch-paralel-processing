package com.mertalptekin.springbatchparalelprocessing;

import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;

import java.util.HashMap;
import java.util.Map;

// Simple partioner herhangi bir algoritma barındırmadığında uygulamanın düzggün çalışabilmesi için Customer Partioner yazdık.
public class CustomPartitioner implements Partitioner {

    // Not: Partition yapıları runtimeda execution context üzerinden yazdığımız algoritmaya göre partionlar oluşturmamızı ve bu partion değeleri üzerinden okuma işlemlerini birbirinden izole etmemiz sağlar
    @Override
    public Map<String, ExecutionContext> partition(int gridSize) {

        // int gridSize -> Klasör içerisindeki dosyaların sayısı kadar dinamik yapabiliriz.

        Map<String, ExecutionContext> partitionMap = new HashMap<>();
        for (int i = 1; i <= gridSize; i++) {
            ExecutionContext context = new ExecutionContext();
            context.putInt("partition", i); // Verinin ait olduğu partition numarasını burada belirliyoruz.
            partitionMap.put("partition" + i, context);
        }

        return partitionMap;
    }
}
