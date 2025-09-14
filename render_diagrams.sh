#!/bin/bash

# Script to render all PlantUML diagrams to PNG
# This script removes problematic sprite references temporarily for rendering

# Note: We don't use set -e here because some diagrams may fail to render
# but we want to continue processing the rest

ARCH_DIR="/opt/3T/docs/arch"

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    echo "Error: Docker is not available. PlantUML diagrams cannot be rendered."
    exit 1
fi

# Check if the arch directory exists
if [[ ! -d "$ARCH_DIR" ]]; then
    echo "Error: Architecture directory $ARCH_DIR not found."
    exit 1
fi

cd "$ARCH_DIR"

echo "Rendering PlantUML diagrams..."

success_count=0
total_count=0

# Process each .puml file
for puml_file in *.puml; do
    if [[ -f "$puml_file" ]]; then
        echo "Processing $puml_file..."
        total_count=$((total_count + 1))
        
        # Render with Docker (suppress error output but continue processing)
        if docker run --rm -v "$ARCH_DIR:/work" -w /work plantuml/plantuml:latest -tpng "$puml_file"; then
            
            # PlantUML uses the puml filename for the output PNG
            expected_png="${puml_file%.puml}.png"
            
            if [[ -f "$expected_png" ]]; then
                echo "  ✓ Generated $expected_png"
                success_count=$((success_count + 1))
            else
                # Check if a file with the title name was created instead
                diagram_title=$(grep -m1 "^@startuml" "$puml_file" | sed 's/@startuml[[:space:]]*//' | tr -d '\r')
                if [[ -n "$diagram_title" ]] && [[ -f "${diagram_title}.png" ]]; then
                    mv "${diagram_title}.png" "$expected_png"
                    echo "  ✓ Generated $expected_png (renamed from ${diagram_title}.png)"
                    success_count=$((success_count + 1))
                else
                    echo "  ✗ Failed to generate PNG for $puml_file"
                fi
            fi
        else
            echo "  ✗ Docker rendering failed for $puml_file"
        fi
        
    fi
done

echo "Done! Successfully rendered $success_count out of $total_count PlantUML diagrams."
