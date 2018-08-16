package com.test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Point3D implements WritableComparable<Point3D> {

    public float x, y, z;

    public Point3D(float fx, float fy, float fz) {
        this.x = fx;
        this.y = fy;
        this.z = fz;
    }

    public Point3D() {
        this(0.0f, 0.0f, 0.0f);
    }

    public void readFields(DataInput in) throws IOException {
        x = in.readFloat();
        y = in.readFloat();
        z = in.readFloat();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeFloat(x);
        out.writeFloat(y);
        out.writeFloat(z);
    }

    public String toString() {
        return "X:" + Float.toString(x) + ", "
                + "Y:" + Float.toString(y) + ", "
                + "Z:" + Float.toString(z);
    }

    public float distanceFromOrigin() {
        return (float) Math.sqrt(x * x + y * y + z * z);
    }

    public int compareTo(Point3D other) {
        return Float.compare(distanceFromOrigin(), other.distanceFromOrigin());
    }

    public boolean equals(Object o) {
        if (!(o instanceof Point3D)) {
            return false;
        }
        Point3D other = (Point3D) o;
        return this.x == other.x && this.y == other.y && this.z == other.z;
    }

    /* 实现 hashCode() 方法很重要
     * Hadoop的Partitioners会用到这个方法，后面再说
     */
    public int hashCode() {
        return Float.floatToIntBits(x) ^ Float.floatToIntBits(y) ^ Float.floatToIntBits(z);
    }
}