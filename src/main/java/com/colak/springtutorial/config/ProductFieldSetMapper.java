package com.colak.springtutorial.config;

import com.colak.springtutorial.dto.ProductDTO;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;

public class ProductFieldSetMapper implements FieldSetMapper<ProductDTO> {

    @Override
    public ProductDTO mapFieldSet(FieldSet fieldSet) {
        return new ProductDTO(
                fieldSet.readLong("productId"),
                fieldSet.readString("productName"),
                fieldSet.readString("productBrand"),
                fieldSet.readDouble("price"),
                fieldSet.readString("description"));
    }
}
