package it.model;

import it.model.avro.SpecificAvroUser;
import org.json.JSONObject;

import java.util.Objects;

public class UserDTO {

    private String id;
    private String name;
    private String surname;

    public UserDTO() {

    }

    public UserDTO(SpecificAvroUser user) {
        this.setId(user.getId().toString());
        this.setName(user.getName().toString());
        this.setSurname(user.getSurname().toString());

    }

    public SpecificAvroUser toAvro(){
        SpecificAvroUser specificAvroUser = new SpecificAvroUser();
        specificAvroUser.setId(this.getId());
        specificAvroUser.setName(this.getName());
        specificAvroUser.setSurname(this.getSurname());
        return specificAvroUser;
    }

    @Override
    public String toString() {
        return new JSONObject(this).toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof UserDTO)) return false;
        UserDTO userDTO = (UserDTO) o;
        return Objects.equals(id, userDTO.id) &&
                Objects.equals(name, userDTO.name) &&
                Objects.equals(surname, userDTO.surname);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, name, surname);
    }

    public String getId() {

        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSurname() {
        return surname;
    }

    public void setSurname(String surname) {
        this.surname = surname;
    }
}
